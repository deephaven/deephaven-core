package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.RangeJoinMatch;
import io.deephaven.api.Strings;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.api.Pair;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.exceptions.OperationException;
import io.deephaven.engine.exceptions.OutOfOrderException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MemoizedOperationKey;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.OperationSnapshotControl;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.join.dupcompact.DupCompactKernel;
import io.deephaven.engine.table.impl.sort.IntSortKernel;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerSparseArraySource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.aggregate.AggregateColumnSource;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.table.impl.util.JobScheduler.IterateAction;
import io.deephaven.engine.table.impl.util.compact.CompactKernel;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.deephaven.base.ArrayUtil.MAX_ARRAY_SIZE;
import static io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation.allSupportParallelPopulation;
import static io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation.prepareAll;
import static io.deephaven.engine.table.impl.by.AggregationProcessor.EXPOSED_GROUP_ROW_SETS;
import static io.deephaven.engine.table.impl.sources.InMemoryColumnSource.getImmutableMemoryColumnSource;
import static io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource.maybeRedirect;

/**
 * Implementation for {@link QueryTable#rangeJoin(Table, Collection, RangeJoinMatch, Collection)}.
 */
public class RangeJoinOperation implements QueryTable.MemoizableOperation<QueryTable> {

    private static final ColumnName LEFT_ROW_SET = EXPOSED_GROUP_ROW_SETS;
    private static final ColumnName RIGHT_ROW_SET = ColumnName.of("__RIGHT_ROW_SET__");

    private static final int MAX_LEFT_CHUNK_CAPACITY = ArrayBackedColumnSource.BLOCK_SIZE;

    private static final String MAXIMUM_STATIC_MEMORY_OVERHEAD_PROPERTY = "RangeJoin.maximumStaticMemoryOverhead";
    private static final double MAXIMUM_STATIC_MEMORY_OVERHEAD = Configuration.getInstance()
            .getDoubleWithDefault(MAXIMUM_STATIC_MEMORY_OVERHEAD_PROPERTY, 1.1);

    private final QueryTable leftTable;
    private final Table rightTable;
    private final Collection<? extends JoinMatch> exactMatches;
    private final RangeJoinMatch rangeMatch;
    private final Collection<? extends Aggregation> aggregations;

    private final String description;
    private final MemoizedOperationKey memoizedOperationKey;
    private final Class<?> rangeValueType;

    public RangeJoinOperation(
            @NotNull final QueryTable leftTable,
            @NotNull final Table rightTable,
            @NotNull final Collection<? extends JoinMatch> exactMatches,
            @NotNull final RangeJoinMatch rangeMatch,
            @NotNull final Collection<? extends Aggregation> aggregations) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.exactMatches = exactMatches;
        this.rangeMatch = rangeMatch;
        this.aggregations = aggregations;

        description = String.format(
                "rangeJoin[leftTable=%s, rightTable=%s, exactMatches=%s, rangeMatch=%s, aggregations=%s]",
                leftTable.getDescription(),
                rightTable.getDescription(),
                Strings.ofJoinMatches(exactMatches),
                Strings.of(rangeMatch),
                Strings.ofAggregations(aggregations));
        memoizedOperationKey = MemoizedOperationKey.rangeJoin(rightTable, exactMatches, rangeMatch, aggregations);

        if (leftTable.isRefreshing() || rightTable.isRefreshing()) {
            throw new UnsupportedOperationException(String.format(
                    "%s: rangeJoin only supports static (not refreshing) inputs at this time: left table is %s, right table is %s",
                    description,
                    leftTable.isRefreshing() ? "refreshing" : "static",
                    rightTable.isRefreshing() ? "refreshing" : "static"));
        }
        validateExactMatchColumns();
        rangeValueType = validateRangeMatchColumns();
        SupportedRangeJoinAggregations.validate(description, aggregations);
    }

    /**
     * Validate that the exact match columns exist and have the same type on both sides for each match.
     */
    private void validateExactMatchColumns() {
        final TableDefinition leftTableDefinition = leftTable.getDefinition();
        final TableDefinition rightTableDefinition = rightTable.getDefinition();
        List<String> issues = null;
        for (final JoinMatch exactMatch : exactMatches) {
            final ColumnDefinition<?> leftColumnDefinition = leftTableDefinition.getColumn(exactMatch.left().name());
            final ColumnDefinition<?> rightColumnDefinition = rightTableDefinition.getColumn(exactMatch.right().name());
            if (leftColumnDefinition == null || rightColumnDefinition == null) {
                if (leftColumnDefinition == null) {
                    (issues == null ? issues = new ArrayList<>() : issues).add(String.format(
                            "left table has no column \"%s\"", Strings.of(exactMatch.left())));
                }
                if (rightColumnDefinition == null) {
                    (issues == null ? issues = new ArrayList<>() : issues).add(String.format(
                            "right table has no column \"%s\"", Strings.of(exactMatch.right())));
                }
            } else {
                issues = validateMatchCompatibility(issues, exactMatch.left(), exactMatch.right(),
                        leftColumnDefinition, rightColumnDefinition);
            }
        }
        if (issues != null) {
            throw new IllegalArgumentException(String.format(
                    "%s: Invalid exact matches: %s", description, String.join(", ", issues)));
        }
    }

    /**
     * Validate that the range match columns exist, have the same type, and that that type is valid.
     *
     * @return The range match column value type
     */
    private Class<?> validateRangeMatchColumns() {
        final TableDefinition leftTableDefinition = leftTable.getDefinition();
        final TableDefinition rightTableDefinition = rightTable.getDefinition();
        final ColumnDefinition<?> leftStartColumnDefinition =
                leftTableDefinition.getColumn(rangeMatch.leftStartColumn().name());
        final ColumnDefinition<?> rightRangeColumnDefinition =
                rightTableDefinition.getColumn(rangeMatch.rightRangeColumn().name());
        final ColumnDefinition<?> leftEndColumnDefinition =
                leftTableDefinition.getColumn(rangeMatch.leftEndColumn().name());

        List<String> issues = null;

        if (leftStartColumnDefinition == null) {
            (issues = new ArrayList<>()).add(String.format(
                    "left start column %s is missing", Strings.of(rangeMatch.leftStartColumn())));
        }
        if (rightRangeColumnDefinition == null) {
            (issues == null ? issues = new ArrayList<>() : issues).add(String.format(
                    "right range column %s is missing", Strings.of(rangeMatch.rightRangeColumn())));
        }
        if (leftEndColumnDefinition == null) {
            (issues == null ? issues = new ArrayList<>() : issues).add(String.format(
                    "left start column %s is missing", Strings.of(rangeMatch.leftEndColumn())));
        }
        if (leftStartColumnDefinition != null && rightRangeColumnDefinition != null) {
            issues = validateMatchCompatibility(issues, rangeMatch.leftStartColumn(), rangeMatch.rightRangeColumn(),
                    leftStartColumnDefinition, rightRangeColumnDefinition);
        }
        if (leftEndColumnDefinition != null && rightRangeColumnDefinition != null) {
            issues = validateMatchCompatibility(issues, rangeMatch.leftEndColumn(), rangeMatch.rightRangeColumn(),
                    leftEndColumnDefinition, rightRangeColumnDefinition);
        }
        if (issues != null) {
            throw new IllegalArgumentException(String.format(
                    "%s: Invalid range match %s: %s", description, Strings.of(rangeMatch), String.join(", ", issues)));
        }
        // noinspection DataFlowIssue (if leftStartColumnDefinition were null, we'd have thrown before here)
        final Class<?> rangeValueType = leftStartColumnDefinition.getDataType();
        if (!rangeValueType.isPrimitive() && !Comparable.class.isAssignableFrom(rangeValueType)) {
            throw new IllegalArgumentException(String.format(
                    "%s: Invalid range value type %s, must be primitive or comparable", description, rangeValueType));
        }
        return rangeValueType;
    }

    private static List<String> validateMatchCompatibility(
            @Nullable List<String> issues,
            @NotNull final ColumnName left,
            @NotNull final ColumnName right,
            @NotNull final ColumnDefinition<?> leftColumnDefinition,
            @NotNull final ColumnDefinition<?> rightColumnDefinition) {
        if (leftColumnDefinition.hasCompatibleDataType(rightColumnDefinition)) {
            return issues;
        }
        if (leftColumnDefinition.getComponentType() != null || rightColumnDefinition.getComponentType() != null) {
            (issues == null ? issues = new ArrayList<>() : issues).add(String.format(
                    "left table column \"%s\" (data type %s, component type %s), is incompatible with right table column \"%s\" (data type %s, component type %s)",
                    Strings.of(left),
                    leftColumnDefinition.getDataType().getName(),
                    Optional.ofNullable(leftColumnDefinition.getComponentType())
                            .map(Class::getName).orElse("null"),
                    Strings.of(right),
                    rightColumnDefinition.getDataType().getName(),
                    Optional.ofNullable(rightColumnDefinition.getComponentType())
                            .map(Class::getName).orElse("null")));
        } else {
            (issues == null ? issues = new ArrayList<>() : issues).add(String.format(
                    "left table column \"%s\" (data type %s), is incompatible with right table column \"%s\" (data type %s)",
                    Strings.of(left),
                    leftColumnDefinition.getDataType(),
                    Strings.of(right),
                    rightColumnDefinition.getDataType()));
        }
        return issues;
    }

    @Override
    public boolean snapshotNeeded() {
        // This operation currently requires the UGP lock when either input table is refreshing, so there's no need to
        // use a snapshot.
        return false;
    }

    @Override
    public OperationSnapshotControl newSnapshotControl(@NotNull final QueryTable queryTable) {
        // Since this operation never needs a snapshot, it does not need to support creating a SnapshotControl.
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getLogPrefix() {
        return "rangeJoin";
    }

    @Override
    public Result<QueryTable> initialize(final boolean usePrev, final long beforeClock) {
        QueryTable.checkInitiateBinaryOperation(leftTable, rightTable);

        final JobScheduler jobScheduler;
        if (ExecutionContext.getContext().getOperationInitializer().canParallelize()) {
            jobScheduler = new OperationInitializerJobScheduler();
        } else {
            jobScheduler = ImmediateJobScheduler.INSTANCE;
        }

        final ExecutionContext executionContext = ExecutionContext.newBuilder()
                .markSystemic().build();

        return new Result<>(staticRangeJoin(jobScheduler, executionContext));
    }

    @Override
    public MemoizedOperationKey getMemoizedOperationKey() {
        return memoizedOperationKey;
    }

    private QueryTable staticRangeJoin(
            @NotNull final JobScheduler jobScheduler,
            @NotNull final ExecutionContext executionContext) {
        final CompletableFuture<QueryTable> resultFuture = new CompletableFuture<>();
        new StaticRangeJoinPhase1(jobScheduler, executionContext, resultFuture).start();
        try {
            return resultFuture.get();
        } catch (InterruptedException e) {
            throw new CancellationException(String.format("%s interrupted", description), e);
        } catch (Exception e) {
            throw new OperationException(String.format("%s failed", description), e);
        }
    }

    private static class RangeJoinPhase {

        protected final JobScheduler jobScheduler;
        protected final ExecutionContext executionContext;
        protected final CompletableFuture<QueryTable> resultFuture;

        protected RangeJoinPhase(
                @NotNull final JobScheduler jobScheduler,
                @NotNull final ExecutionContext executionContext,
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            this.jobScheduler = jobScheduler;
            this.executionContext = executionContext;
            this.resultFuture = resultFuture;
        }
    }

    private class StaticRangeJoinPhase1 extends RangeJoinPhase {

        private StaticRangeJoinPhase1(
                @NotNull final JobScheduler jobScheduler,
                @NotNull final ExecutionContext executionContext,
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            super(jobScheduler, executionContext, resultFuture);
        }

        private void start() {
            // Perform the left table work via the job scheduler, possibly concurrently with the right table work.
            final CompletableFuture<Table> groupLeftTableFuture = new CompletableFuture<>();
            jobScheduler.submit(
                    executionContext,
                    () -> groupLeftTableFuture.complete(groupLeftTable()),
                    logOutput -> logOutput.append("static range join group left table"),
                    groupLeftTableFuture::completeExceptionally);
            // Perform the right table work on this thread. We don't need to involve the scheduler, and this way we may
            // be able to exploit filter parallelism.
            final Table rightTableGrouped;
            try {
                rightTableGrouped = filterAndGroupRightTable();
            } catch (Exception e) {
                // Try to ensure that the group-left-table job is no longer running before re-throwing
                groupLeftTableFuture.cancel(true);
                try {
                    groupLeftTableFuture.get();
                } catch (Exception ignored) {
                }
                resultFuture.completeExceptionally(e);
                return;
            }
            final Table leftTableGrouped;
            try {
                leftTableGrouped = groupLeftTableFuture.get();
            } catch (Exception e) {
                resultFuture.completeExceptionally(e);
                return;
            }
            new StaticRangeJoinPhase2(jobScheduler, executionContext, resultFuture).start(
                    leftTableGrouped,
                    rightTableGrouped);
        }

        private Table groupLeftTable() {
            return exposeGroupRowSets(leftTable, JoinMatch.lefts(exactMatches));
        }

        private Table filterAndGroupRightTable() {
            final Table rightTableCoalesced = rightTable.coalesce();
            final Table rightTableFiltered;
            if (rangeValueType == double.class || rangeValueType == float.class) {
                rightTableFiltered = rightTableCoalesced.where(
                        new ValidFloatingPointFilter(rangeMatch.rightRangeColumn()));
            } else {
                rightTableFiltered = rightTableCoalesced.where(Filter.isNotNull(rangeMatch.rightRangeColumn()));
            }
            return exposeGroupRowSets((QueryTable) rightTableFiltered, JoinMatch.rights(exactMatches));
        }
    }

    private static QueryTable exposeGroupRowSets(
            @NotNull final QueryTable inputTable,
            @NotNull final Collection<ColumnName> exactMatches) {
        return inputTable.aggNoMemo(AggregationProcessor.forExposeGroupRowSets(), false, null, exactMatches);
    }

    private class StaticRangeJoinPhase2
            extends RangeJoinPhase
            implements IterateAction<StaticRangeJoinPhase2.TaskContext> {

        // Derived from operation inputs
        private final ColumnSource<?> leftStartValues;
        private final ColumnSource<?> rightRangeValues;
        private final ColumnSource<?> leftEndValues;
        private final ChunkType valueChunkType;
        private final DupCompactKernel valueChunkDupCompactKernel;
        private final RangeSearchKernel rangeSearchKernel;
        private final CompactKernel valueChunkCompactKernel;

        // Output fields:
        // When output is redirected, outputRedirection is non-null and the (non-null) "inner" sources are used for
        // dense storage, redirected from the left table's row key space to a flat space.
        // Otherwise, the outputRedirection and the "inner" sources are null, and the "exposed" sources are used
        // directly for sparse storage in the left table's row key space.
        // "Exposed" sources are the output used for constructing our result aggregations.
        private final RowRedirection outputRedirection;
        private final WritableColumnSource<Integer> outputSlotsInner;
        private final WritableColumnSource<Integer> outputSlotsExposed;
        private final WritableColumnSource<Integer> outputStartPositionsInclusiveInner;
        private final WritableColumnSource<Integer> outputStartPositionsInclusiveExposed;
        private final WritableColumnSource<Integer> outputEndPositionsExclusiveInner;
        private final WritableColumnSource<Integer> outputEndPositionsExclusiveExposed;

        // Derived from phase inputs
        private ColumnSource<RowSet> leftGroupRowSets;
        private ColumnSource<RowSet> rightGroupRowSets;

        private StaticRangeJoinPhase2(
                @NotNull final JobScheduler jobScheduler,
                @NotNull final ExecutionContext executionContext,
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            super(jobScheduler, executionContext, resultFuture);

            leftStartValues = ReinterpretUtils.maybeConvertToPrimitive(
                    leftTable.getColumnSource(rangeMatch.leftStartColumn().name()));
            rightRangeValues = ReinterpretUtils.maybeConvertToPrimitive(
                    rightTable.getColumnSource(rangeMatch.rightRangeColumn().name()));
            leftEndValues = ReinterpretUtils.maybeConvertToPrimitive(
                    leftTable.getColumnSource(rangeMatch.leftEndColumn().name()));
            valueChunkType = leftStartValues.getChunkType();
            Assert.eq(valueChunkType, "valueChunkType",
                    rightRangeValues.getChunkType(), "rightRangeValues.getChunkType()");
            Assert.eq(valueChunkType, "valueChunkType",
                    leftEndValues.getChunkType(), "leftEndValues.getChunkType()");
            valueChunkDupCompactKernel = DupCompactKernel.makeDupCompactNaturalOrdering(valueChunkType, false);
            valueChunkCompactKernel = CompactKernel.makeCompact(valueChunkType);
            rangeSearchKernel = RangeSearchKernel.makeRangeSearchKernel(
                    valueChunkType, rangeMatch.rangeStartRule(), rangeMatch.rangeEndRule());

            final boolean leftIsFlat = leftTable.isFlat();
            if (!leftIsFlat && SparseConstants.sparseStructureExceedsOverhead(
                    leftTable.getRowSet(), MAXIMUM_STATIC_MEMORY_OVERHEAD)) {
                outputRedirection = new InverseWrappedRowSetRowRedirection(leftTable.getRowSet());
                outputSlotsInner = allocateIntOutputSource(true);
                outputStartPositionsInclusiveInner = allocateIntOutputSource(true);
                outputEndPositionsExclusiveInner = allocateIntOutputSource(true);

                Assert.assertion(allSupportParallelPopulation(
                        outputSlotsInner, outputStartPositionsInclusiveInner, outputEndPositionsExclusiveInner),
                        "All output inner sources support parallel population");
                try (final RowSet flatOutputRowSet = RowSetFactory.flat(leftTable.size())) {
                    prepareAll(flatOutputRowSet,
                            outputSlotsInner, outputStartPositionsInclusiveInner, outputEndPositionsExclusiveInner);
                }
                outputSlotsExposed = maybeRedirect(
                        outputRedirection, outputSlotsInner, leftTable.size() - 1);
                outputStartPositionsInclusiveExposed = maybeRedirect(
                        outputRedirection, outputStartPositionsInclusiveInner, leftTable.size() - 1);
                outputEndPositionsExclusiveExposed = maybeRedirect(
                        outputRedirection, outputEndPositionsExclusiveInner, leftTable.size() - 1);
            } else {
                outputRedirection = null;
                outputSlotsInner = null;
                outputStartPositionsInclusiveInner = null;
                outputEndPositionsExclusiveInner = null;
                outputSlotsExposed = allocateIntOutputSource(leftIsFlat);
                outputStartPositionsInclusiveExposed = allocateIntOutputSource(leftIsFlat);
                outputEndPositionsExclusiveExposed = allocateIntOutputSource(leftIsFlat);
                Assert.assertion(allSupportParallelPopulation(
                        outputSlotsExposed, outputStartPositionsInclusiveExposed, outputEndPositionsExclusiveExposed),
                        "All output exposed sources support parallel population");
                prepareAll(leftTable.getRowSet(),
                        outputSlotsExposed, outputStartPositionsInclusiveExposed, outputEndPositionsExclusiveExposed);
            }
        }

        private WritableColumnSource<Integer> allocateIntOutputSource(final boolean flat) {
            return flat
                    ? getImmutableMemoryColumnSource(leftTable.size(), int.class, null)
                    : new IntegerSparseArraySource();
        }

        private void start(@NotNull final Table leftTableGrouped, @NotNull final Table rightTableGrouped) {
            final Table joinedInputTables = leftTableGrouped.naturalJoin(
                    rightTableGrouped, exactMatches, List.of(JoinAddition.of(RIGHT_ROW_SET, EXPOSED_GROUP_ROW_SETS)));
            leftGroupRowSets = joinedInputTables.getColumnSource(LEFT_ROW_SET.name(), RowSet.class);
            rightGroupRowSets = joinedInputTables.getColumnSource(RIGHT_ROW_SET.name(), RowSet.class);
            jobScheduler.iterateParallel(
                    executionContext,
                    logOutput -> logOutput.append("static range join find ranges"),
                    TaskContext::new,
                    0,
                    joinedInputTables.intSize(),
                    this,
                    () -> new StaticRangeJoinPhase3(jobScheduler, executionContext, resultFuture).start(
                            rightGroupRowSets,
                            outputSlotsExposed,
                            outputStartPositionsInclusiveExposed,
                            outputEndPositionsExclusiveExposed),
                    resultFuture::completeExceptionally);
        }

        private class TaskContext implements JobScheduler.JobThreadContext {

            private static final int CLOSED_SENTINEL = -1;

            // Left resources, size bounded by leftChunkCapacity
            private int leftChunkCapacity;
            private SharedContext leftSharedContext;
            private ChunkSource.FillContext leftStartValuesFillContext;
            private ChunkSource.FillContext leftEndValuesFillContext;
            private WritableChunk<Values> leftStartValuesChunk;
            private WritableChunk<Values> leftEndValuesChunk;
            private WritableBooleanChunk<Any> leftValidity;
            private WritableIntChunk<ChunkPositions> leftChunkPositions;
            private IntSortKernel<Values, ChunkPositions> leftSortKernel;

            // Right resources, size bounded by rightChunkCapacity
            private int rightChunkCapacity;
            private ChunkSource.FillContext rightRangeValuesFillContext;
            private WritableChunk<Values> rightRangeValuesChunk;
            private WritableIntChunk<ChunkPositions> rightStartOffsets;
            private WritableIntChunk<ChunkLengths> rightLengths;

            // Output resources, size bounded by leftChunkCapacity
            private ChunkSink.FillFromContext outputSlotsFillFromContext;
            private ChunkSink.FillFromContext outputStartPositionsInclusiveFillFromContext;
            private ChunkSink.FillFromContext outputEndPositionsExclusiveFillFromContext;
            private WritableIntChunk<? extends Values> outputSlotsChunk;
            private WritableIntChunk<? extends Values> outputStartPositionsInclusiveChunk;
            private WritableIntChunk<? extends Values> outputEndPositionsExclusiveChunk;

            private TaskContext() {
                // All resources are resizable. Nothing is allocated until needed.
            }

            private void ensureLeftCapacity(final long leftGroupSize) {
                if (leftChunkCapacity == CLOSED_SENTINEL) {
                    throw new IllegalStateException(String.format(
                            "%s: used %s after close", description, this.getClass()));
                }
                if (leftChunkCapacity >= leftGroupSize) {
                    return;
                }
                if (leftChunkCapacity > 0) {
                    final SafeCloseable[] toClose = new SafeCloseable[] {
                            leftSharedContext, // Do close() leftSharedContext
                            leftStartValuesFillContext,
                            leftEndValuesFillContext,
                            leftStartValuesChunk,
                            leftEndValuesChunk,
                            leftValidity,
                            leftChunkPositions,
                            leftSortKernel,

                            outputSlotsFillFromContext,
                            outputStartPositionsInclusiveFillFromContext,
                            outputEndPositionsExclusiveFillFromContext,
                            outputSlotsChunk,
                            outputStartPositionsInclusiveChunk,
                            outputEndPositionsExclusiveChunk
                    };

                    leftChunkCapacity = 0; // Record that we don't want to re-close

                    // Don't null out the leftSharedContext; close() is sufficient to empty it for re-use
                    leftStartValuesFillContext = null;
                    leftEndValuesFillContext = null;
                    leftStartValuesChunk = null;
                    leftEndValuesChunk = null;
                    leftValidity = null;
                    leftChunkPositions = null;
                    leftSortKernel = null;

                    outputSlotsFillFromContext = null;
                    outputStartPositionsInclusiveFillFromContext = null;
                    outputEndPositionsExclusiveFillFromContext = null;
                    outputSlotsChunk = null;
                    outputStartPositionsInclusiveChunk = null;
                    outputEndPositionsExclusiveChunk = null;

                    SafeCloseable.closeAll(toClose);
                }

                leftChunkCapacity = (int) Math.min(MAX_LEFT_CHUNK_CAPACITY, leftGroupSize);

                if (leftSharedContext == null) { // We can re-use a SharedContext after close(), no need to re-allocate
                    leftSharedContext = SharedContext.makeSharedContext();
                }

                leftStartValuesFillContext = leftStartValues.makeFillContext(leftChunkCapacity, leftSharedContext);
                leftEndValuesFillContext = leftEndValues.makeFillContext(leftChunkCapacity, leftSharedContext);
                leftStartValuesChunk = valueChunkType.makeWritableChunk(leftChunkCapacity);
                leftEndValuesChunk = valueChunkType.makeWritableChunk(leftChunkCapacity);
                leftValidity = WritableBooleanChunk.makeWritableChunk(leftChunkCapacity);
                leftChunkPositions = WritableIntChunk.makeWritableChunk(leftChunkCapacity);
                leftSortKernel = IntSortKernel.makeContext(
                        valueChunkType, SortingOrder.Ascending, leftChunkCapacity, true);

                if (outputRedirection == null) {
                    // We'll be filling exposed, sparse sources directly
                    outputSlotsFillFromContext =
                            outputSlotsExposed.makeFillFromContext(leftChunkCapacity);
                    outputStartPositionsInclusiveFillFromContext =
                            outputStartPositionsInclusiveExposed.makeFillFromContext(leftChunkCapacity);
                    outputEndPositionsExclusiveFillFromContext =
                            outputEndPositionsExclusiveExposed.makeFillFromContext(leftChunkCapacity);
                } else {
                    // We'll be filling the inner, dense sources
                    // noinspection DataFlowIssue
                    outputSlotsFillFromContext =
                            outputSlotsInner.makeFillFromContext(leftChunkCapacity);
                    // noinspection DataFlowIssue
                    outputStartPositionsInclusiveFillFromContext =
                            outputStartPositionsInclusiveInner.makeFillFromContext(leftChunkCapacity);
                    // noinspection DataFlowIssue
                    outputEndPositionsExclusiveFillFromContext =
                            outputEndPositionsExclusiveInner.makeFillFromContext(leftChunkCapacity);
                }
                outputSlotsChunk = WritableIntChunk.makeWritableChunk(leftChunkCapacity);
                outputStartPositionsInclusiveChunk = WritableIntChunk.makeWritableChunk(leftChunkCapacity);
                outputEndPositionsExclusiveChunk = WritableIntChunk.makeWritableChunk(leftChunkCapacity);
            }

            private void ensureRightCapacity(final long rightGroupSize) {
                if (rightChunkCapacity == CLOSED_SENTINEL) {
                    throw new IllegalStateException(String.format(
                            "%s: used %s after close", description, this.getClass()));
                }
                if (rightGroupSize > MAX_ARRAY_SIZE) {
                    throw new IllegalArgumentException(
                            String.format("%s: Unable to process right table bucket larger than %d, encountered %d",
                                    description, MAX_ARRAY_SIZE, rightGroupSize));
                }
                if (rightChunkCapacity >= rightGroupSize) {
                    return;
                }
                if (rightChunkCapacity > 0) {
                    final SafeCloseable[] toClose = new SafeCloseable[] {
                            rightRangeValuesFillContext,
                            rightRangeValuesChunk,
                            rightStartOffsets,
                            rightLengths
                    };

                    rightChunkCapacity = 0; // Record that we don't want to re-close

                    rightRangeValuesFillContext = null;
                    rightRangeValuesChunk = null;
                    rightStartOffsets = null;
                    rightLengths = null;

                    SafeCloseable.closeAll(toClose);
                }

                rightChunkCapacity = (int) Math.min(MAX_ARRAY_SIZE, 1L << MathUtil.ceilLog2(rightGroupSize));

                rightRangeValuesFillContext = rightRangeValues.makeFillContext(rightChunkCapacity);
                rightRangeValuesChunk = valueChunkType.makeWritableChunk(rightChunkCapacity);
                rightStartOffsets = WritableIntChunk.makeWritableChunk(rightChunkCapacity);
                rightLengths = WritableIntChunk.makeWritableChunk(rightChunkCapacity);
            }

            @Override
            public void close() {
                if (rightChunkCapacity == CLOSED_SENTINEL) {
                    throw new IllegalStateException(String.format(
                            "%s: closed %s more than once", description, this.getClass()));
                }
                leftChunkCapacity = rightChunkCapacity = CLOSED_SENTINEL;
                SafeCloseable.closeAll(
                        // Left resources
                        leftSharedContext,
                        leftStartValuesFillContext,
                        leftEndValuesFillContext,
                        leftStartValuesChunk,
                        leftEndValuesChunk,
                        leftValidity,
                        leftChunkPositions,
                        leftSortKernel,
                        // Right resources
                        rightRangeValuesFillContext,
                        rightRangeValuesChunk,
                        rightStartOffsets,
                        rightLengths,
                        // Output resources
                        outputSlotsFillFromContext,
                        outputSlotsChunk,
                        outputStartPositionsInclusiveFillFromContext,
                        outputStartPositionsInclusiveChunk,
                        outputEndPositionsExclusiveFillFromContext,
                        outputEndPositionsExclusiveChunk);
            }
        }

        @Override
        public void run(
                @NotNull final TaskContext tc,
                final int index,
                @NotNull final Consumer<Exception> nestedErrorConsumer) {
            final RowSet leftRows = leftGroupRowSets.get(index);
            assert leftRows != null;
            tc.ensureLeftCapacity(leftRows.size());

            final RowSet rightRows = rightGroupRowSets.get(index);
            final int rightSize = rightRows == null ? 0 : rightRows.intSize();
            if (rightSize != 0) {
                // Read right rows
                tc.ensureRightCapacity(rightSize);
                rightRangeValues.fillChunk(tc.rightRangeValuesFillContext, tc.rightRangeValuesChunk, rightRows);

                // Find and compact right runs, verifying order
                ChunkUtils.fillInOrder(tc.rightStartOffsets);
                final int firstOutOfOrderRightPosition = valueChunkDupCompactKernel.compactDuplicatesPreferFirst(
                        tc.rightRangeValuesChunk, tc.rightStartOffsets);
                if (firstOutOfOrderRightPosition != -1) {
                    throw new OutOfOrderException(String.format(
                            "%s: Encountered out of order data in right table at row key %d",
                            description, rightRows.get(firstOutOfOrderRightPosition)));
                }
            }

            try (final RowSequence.Iterator leftRowsIterator = leftRows.getRowSequenceIterator()) {
                while (leftRowsIterator.hasMore()) {
                    final RowSequence leftRowsSlice =
                            leftRowsIterator.getNextRowSequenceWithLength(MAX_LEFT_CHUNK_CAPACITY);
                    final int sliceSize = leftRowsSlice.intSize();
                    leftStartValues.fillChunk(tc.leftStartValuesFillContext, tc.leftStartValuesChunk, leftRowsSlice);
                    leftEndValues.fillChunk(tc.leftEndValuesFillContext, tc.leftEndValuesChunk, leftRowsSlice);
                    tc.leftSharedContext.reset();

                    if (rightSize != 0) {
                        rangeSearchKernel.processInvalidRanges(
                                tc.leftStartValuesChunk, tc.leftEndValuesChunk, tc.leftValidity,
                                tc.outputStartPositionsInclusiveChunk, tc.outputEndPositionsExclusiveChunk);
                        valueChunkCompactKernel.compact(tc.leftStartValuesChunk, tc.leftValidity);
                        valueChunkCompactKernel.compact(tc.leftEndValuesChunk, tc.leftValidity);

                        ChunkUtils.fillWithValidPositions(tc.leftChunkPositions, tc.leftValidity);
                        tc.leftSortKernel.sort(tc.leftChunkPositions, tc.leftStartValuesChunk);
                        rangeSearchKernel.processRangeStarts(tc.leftStartValuesChunk, tc.leftChunkPositions,
                                tc.rightRangeValuesChunk, tc.rightStartOffsets, rightSize,
                                tc.outputStartPositionsInclusiveChunk);

                        ChunkUtils.fillWithValidPositions(tc.leftChunkPositions, tc.leftValidity);
                        tc.leftSortKernel.sort(tc.leftChunkPositions, tc.leftEndValuesChunk);
                        rangeSearchKernel.processRangeEnds(tc.leftEndValuesChunk, tc.leftChunkPositions,
                                tc.rightRangeValuesChunk, tc.rightStartOffsets, rightSize,
                                tc.outputEndPositionsExclusiveChunk);
                    } else {
                        rangeSearchKernel.processAllRangesForEmptyRight(
                                tc.leftStartValuesChunk, tc.leftEndValuesChunk,
                                tc.outputStartPositionsInclusiveChunk, tc.outputEndPositionsExclusiveChunk);
                    }

                    tc.outputSlotsChunk.fillWithValue(0, sliceSize, index);
                    tc.outputSlotsChunk.setSize(sliceSize);

                    if (outputRedirection == null) {
                        outputSlotsExposed.fillFromChunk(
                                tc.outputSlotsFillFromContext,
                                tc.outputSlotsChunk,
                                leftRowsSlice);
                        outputStartPositionsInclusiveExposed.fillFromChunk(
                                tc.outputStartPositionsInclusiveFillFromContext,
                                tc.outputStartPositionsInclusiveChunk,
                                leftRowsSlice);
                        outputEndPositionsExclusiveExposed.fillFromChunk(
                                tc.outputEndPositionsExclusiveFillFromContext,
                                tc.outputEndPositionsExclusiveChunk,
                                leftRowsSlice);
                    } else {
                        // @formatter:off
                        try (final RowSet leftSliceRowSet = leftRowsSlice.asRowSet();
                             final RowSequence invertedLeftRowsSlice = leftTable.getRowSet().invert(leftSliceRowSet)) {
                            // @formatter:on
                            outputSlotsInner.fillFromChunk(
                                    tc.outputSlotsFillFromContext,
                                    tc.outputSlotsChunk,
                                    invertedLeftRowsSlice);
                            outputStartPositionsInclusiveInner.fillFromChunk(
                                    tc.outputStartPositionsInclusiveFillFromContext,
                                    tc.outputStartPositionsInclusiveChunk,
                                    invertedLeftRowsSlice);
                            outputEndPositionsExclusiveInner.fillFromChunk(
                                    tc.outputEndPositionsExclusiveFillFromContext,
                                    tc.outputEndPositionsExclusiveChunk,
                                    invertedLeftRowsSlice);
                        }
                    }
                }
            }
        }
    }

    private class StaticRangeJoinPhase3 extends RangeJoinPhase {

        private StaticRangeJoinPhase3(
                @NotNull final JobScheduler jobScheduler,
                @NotNull final ExecutionContext executionContext,
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            super(jobScheduler, executionContext, resultFuture);
        }

        public void start(
                @NotNull final ColumnSource<RowSet> rightGroupRowSets,
                @NotNull final ColumnSource<Integer> outputSlots,
                @NotNull final ColumnSource<Integer> outputStartPositionsInclusive,
                @NotNull final ColumnSource<Integer> outputEndPositionsExclusive) {
            // We support only ColumnAggregation(s) with spec of type AggSpecGroup at this time. Since we validate our
            // inputs in the RangeJoinOperation constructor, we can proceed here using just input/output pairs, knowing
            // that all are for a "group" aggregation.
            final List<Pair> groupPairs = AggregationPairs.of(aggregations).collect(Collectors.toList());
            final ColumnSource<RowSet> outputRowSets =
                    maybeRedirect(new IntColumnSourceRowRedirection<>(outputSlots), rightGroupRowSets);
            final Map<String, ColumnSource<?>> resultColumnSources =
                    new LinkedHashMap<>(leftTable.getColumnSourceMap());
            groupPairs.forEach((final Pair groupPair) -> resultColumnSources.put(
                    groupPair.output().name(),
                    AggregateColumnSource.forRangeJoin(
                            rightTable.getColumnSource(groupPair.input().name()),
                            outputRowSets,
                            outputStartPositionsInclusive,
                            outputEndPositionsExclusive)));
            resultFuture.complete(new QueryTable(leftTable.getRowSet(), resultColumnSources));
        }
    }
}
