package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.RangeJoinMatch;
import io.deephaven.api.Strings;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.api.agg.Pair;
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
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.SwapListener;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.sort.IntSortKernel;
import io.deephaven.engine.table.impl.sort.findruns.FindRunsKernel;
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

    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

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
            if (leftColumnDefinition == null) {
                if (rightColumnDefinition == null) {
                    (issues == null ? issues = new ArrayList<>() : issues).add(
                            String.format("both columns from %s are missing", Strings.of(exactMatch)));
                } else {
                    (issues == null ? issues = new ArrayList<>() : issues).add(
                            String.format("left column from %s is missing", Strings.of(exactMatch)));
                }
            } else if (rightColumnDefinition == null) {
                (issues == null ? issues = new ArrayList<>() : issues).add(
                        String.format("right column from %s is missing", Strings.of(exactMatch)));
            } else if (!leftColumnDefinition.hasCompatibleDataType(rightColumnDefinition)) {
                (issues == null ? issues = new ArrayList<>() : issues).add(
                        String.format("incompatible columns in %s: left has (%s, %s) and right has (%s, %s)",
                                Strings.of(exactMatch),
                                leftColumnDefinition.getDataType(),
                                leftColumnDefinition.getComponentType(),
                                rightColumnDefinition.getDataType(),
                                rightColumnDefinition.getComponentType()));
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
                    "left start column %s is missing", rangeMatch.leftStartColumn().name()));
        }
        if (rightRangeColumnDefinition == null) {
            (issues == null ? issues = new ArrayList<>() : issues).add(String.format(
                    "right range column %s is missing", rangeMatch.rightRangeColumn().name()));
        }
        if (leftEndColumnDefinition == null) {
            (issues == null ? issues = new ArrayList<>() : issues).add(String.format(
                    "left start column %s is missing", rangeMatch.leftEndColumn().name()));
        }
        if (leftStartColumnDefinition != null
                && rightRangeColumnDefinition != null
                && !leftStartColumnDefinition.hasCompatibleDataType(rightRangeColumnDefinition)) {
            (issues == null ? issues = new ArrayList<>() : issues).add(String.format(
                    "incompatible columns for range start: left has (%s, %s) and right has (%s, %s)",
                    leftStartColumnDefinition.getDataType(),
                    leftStartColumnDefinition.getComponentType(),
                    rightRangeColumnDefinition.getDataType(),
                    rightRangeColumnDefinition.getComponentType()));
        }
        if (leftEndColumnDefinition != null
                && rightRangeColumnDefinition != null
                && !leftEndColumnDefinition.hasCompatibleDataType(rightRangeColumnDefinition)) {
            (issues == null ? issues = new ArrayList<>() : issues).add(String.format(
                    "incompatible columns for range end: left has (%s, %s) and right has (%s, %s)",
                    leftEndColumnDefinition.getDataType(),
                    leftEndColumnDefinition.getComponentType(),
                    rightRangeColumnDefinition.getDataType(),
                    rightRangeColumnDefinition.getComponentType()));
        }
        if (issues != null) {
            throw new IllegalArgumentException(String.format(
                    "%s: Invalid range match %s: %s", description, Strings.of(rangeMatch), String.join(", ", issues)));
        }

        final Class<?> rangeValueType = leftStartColumnDefinition.getDataType();
        if (!rangeValueType.isPrimitive() && !Comparable.class.isAssignableFrom(rangeValueType)) {
            throw new IllegalArgumentException(String.format(
                    "%s: Invalid range value type %s, must be primitive or comparable", description, rangeValueType));
        }
        return rangeValueType;
    }

    @Override
    public boolean snapshotNeeded() {
        // This operation currently requires the UGP lock when either input table is refreshing, so there's no need to
        // use a snapshot.
        return false;
    }

    @Override
    public SwapListener newSwapListener(@NotNull final QueryTable queryTable) {
        // Since this operation never needs a snapshot, it does not need to support creating a SwapListener.
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
        if (OperationInitializationThreadPool.NUM_THREADS > 1
                && !OperationInitializationThreadPool.isInitializationThread()) {
            jobScheduler = new OperationInitializationPoolJobScheduler();
        } else {
            jobScheduler = ImmediateJobScheduler.INSTANCE;
        }

        return new Result<>(staticRangeJoin(jobScheduler));
    }

    @Override
    public MemoizedOperationKey getMemoizedOperationKey() {
        return memoizedOperationKey;
    }

    private QueryTable staticRangeJoin(@NotNull final JobScheduler jobScheduler) {
        final CompletableFuture<QueryTable> resultFuture = new CompletableFuture<>();
        new StaticRangeJoinPhase1(jobScheduler, resultFuture).start();
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
        protected final CompletableFuture<QueryTable> resultFuture;

        protected RangeJoinPhase(
                @NotNull final JobScheduler jobScheduler,
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            this.jobScheduler = jobScheduler;
            this.resultFuture = resultFuture;
        }
    }

    private class StaticRangeJoinPhase1 extends RangeJoinPhase {

        private Table outputLeftTableGrouped;
        private Table outputRightTableGrouped;

        private StaticRangeJoinPhase1(
                @NotNull final JobScheduler jobScheduler,
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            super(jobScheduler, resultFuture);
        }

        private void start() {
            // Perform the left table work via the job scheduler, possibly concurrently with the right table work.
            final CompletableFuture<?> groupLeftTableFuture = new CompletableFuture<>();
            jobScheduler.submit(
                    ExecutionContext.getContextToRecord(),
                    this::groupLeftTable,
                    logOutput -> logOutput.append("static range join group left table"),
                    groupLeftTableFuture::completeExceptionally);
            // Perform the right table work on this thread. We don't need to involve the scheduler, and this way we may
            // be able to exploit filter parallelism.
            try {
                filterAndGroupRightTable();
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
            try {
                groupLeftTableFuture.get();
            } catch (Exception e) {
                resultFuture.completeExceptionally(e);
                return;
            }
            new StaticRangeJoinPhase2(jobScheduler, resultFuture)
                    .start(outputLeftTableGrouped, outputRightTableGrouped);
        }

        private void groupLeftTable() {
            outputLeftTableGrouped = exposeGroupRowSets(
                    leftTable,
                    JoinMatch.lefts(exactMatches));
        }

        private void filterAndGroupRightTable() {
            final Table rightTableCoalesced = rightTable.coalesce();
            final Table rightTableFiltered;
            if (rangeValueType == double.class || rangeValueType == float.class) {
                rightTableFiltered = rightTableCoalesced.where(String.format("!isNaN(%s) && !isNull(%s)",
                        rangeMatch.rightRangeColumn().name(), rangeMatch.rightRangeColumn().name()));
            } else {
                rightTableFiltered = rightTableCoalesced.where(Filter.isNotNull(rangeMatch.rightRangeColumn()));
            }
            outputRightTableGrouped = exposeGroupRowSets(
                    (QueryTable) rightTableFiltered,
                    JoinMatch.rights(exactMatches));
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

        private final ColumnSource<?> leftStartValues;
        private final ColumnSource<?> rightRangeValues;
        private final ColumnSource<?> leftEndValues;
        private final ChunkType valueChunkType;
        private final RangeSearchKernel rangeSearchKernel;
        private final CompactKernel valueChunkCompactKernel;

        private final RowRedirection outputRedirection;
        private final WritableColumnSource<Integer> outputSlotsInner;
        private final WritableColumnSource<Integer> outputSlotsExposed;
        private final WritableColumnSource<Integer> outputStartPositionsInclusiveInner;
        private final WritableColumnSource<Integer> outputStartPositionsInclusiveExposed;
        private final WritableColumnSource<Integer> outputEndPositionsExclusiveInner;
        private final WritableColumnSource<Integer> outputEndPositionsExclusiveExposed;

        private ColumnSource<RowSet> leftGroupRowSets;
        private ColumnSource<RowSet> rightGroupRowSets;

        private StaticRangeJoinPhase2(
                @NotNull final JobScheduler jobScheduler,
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            super(jobScheduler, resultFuture);

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
            rangeSearchKernel = RangeSearchKernel.makeRangeSearchKernel(
                    valueChunkType, rangeMatch.rangeStartRule(), rangeMatch.rangeEndRule());
            valueChunkCompactKernel = CompactKernel.makeCompact(valueChunkType);

            if (!leftTable.isFlat() && SparseConstants.sparseStructureExceedsOverhead(
                    leftTable.getRowSet(), MAXIMUM_STATIC_MEMORY_OVERHEAD)) {
                outputRedirection = new InverseWrappedRowSetRowRedirection(leftTable.getRowSet());
                outputSlotsInner = getImmutableMemoryColumnSource(leftTable.size(), int.class, null);
                outputStartPositionsInclusiveInner = getImmutableMemoryColumnSource(leftTable.size(), int.class, null);
                outputEndPositionsExclusiveInner = getImmutableMemoryColumnSource(leftTable.size(), int.class, null);

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
                outputSlotsExposed = new IntegerSparseArraySource();
                outputStartPositionsInclusiveExposed = new IntegerSparseArraySource();
                outputEndPositionsExclusiveExposed = new IntegerSparseArraySource();
                Assert.assertion(allSupportParallelPopulation(
                        outputSlotsExposed, outputStartPositionsInclusiveExposed, outputEndPositionsExclusiveExposed),
                        "All output exposed sources support parallel population");
                prepareAll(leftTable.getRowSet(),
                        outputSlotsExposed, outputStartPositionsInclusiveExposed, outputEndPositionsExclusiveExposed);
            }
        }

        private void start(@NotNull final Table leftTableGrouped, @NotNull final Table rightTableGrouped) {
            final Table joinedInputTables = leftTableGrouped.naturalJoin(
                    rightTableGrouped, exactMatches, List.of(JoinAddition.of(RIGHT_ROW_SET, EXPOSED_GROUP_ROW_SETS)));
            leftGroupRowSets = joinedInputTables.getColumnSource(LEFT_ROW_SET.name(), RowSet.class);
            rightGroupRowSets = joinedInputTables.getColumnSource(RIGHT_ROW_SET.name(), RowSet.class);
            jobScheduler.iterateParallel(
                    ExecutionContext.getContextToRecord(),
                    logOutput -> logOutput.append("static range join find ranges"),
                    TaskContext::new,
                    0,
                    joinedInputTables.intSize(),
                    this,
                    () -> new StaticRangeJoinPhase3(jobScheduler, resultFuture).start(
                            rightGroupRowSets,
                            outputSlotsExposed,
                            outputStartPositionsInclusiveExposed,
                            outputEndPositionsExclusiveExposed),
                    resultFuture::completeExceptionally);
        }

        private class TaskContext implements JobScheduler.JobThreadContext {

            private static final int CLOSED_SENTINEL = -1;

            // Left resources, all final
            private final SharedContext leftSharedContext;
            private final ChunkSource.FillContext leftStartValuesFillContext;
            private final ChunkSource.FillContext leftEndValuesFillContext;
            private final WritableChunk<Values> leftStartValuesChunk;
            private final WritableChunk<Values> leftEndValuesChunk;
            private final WritableBooleanChunk<Any> leftValidity;
            private final WritableIntChunk<ChunkPositions> leftChunkPositions;
            private final IntSortKernel<Values, ChunkPositions> leftSortKernel;

            // Final right resources
            private final FindRunsKernel rightFindRunsKernel;

            // Resizable right resources
            private int rightChunkSize = 0;
            private ChunkSource.FillContext rightRangeValuesFillContext;
            private WritableChunk<Values> rightRangeValuesChunk;
            private WritableIntChunk<ChunkPositions> rightStartOffsets;
            private WritableIntChunk<ChunkLengths> rightLengths;

            // Output resources
            private final ChunkSink.FillFromContext outputSlotsFillFromContext;
            private final WritableIntChunk<? extends Values> outputSlotsChunk;
            private final ChunkSink.FillFromContext outputStartPositionsInclusiveFillFromContext;
            private final WritableIntChunk<? extends Values> outputStartPositionsInclusiveChunk;
            private final ChunkSink.FillFromContext outputEndPositionsExclusiveFillFromContext;
            private final WritableIntChunk<? extends Values> outputEndPositionsExclusiveChunk;

            private TaskContext() {
                // Left resources
                leftSharedContext = SharedContext.makeSharedContext();
                leftStartValuesFillContext = leftStartValues.makeFillContext(CHUNK_SIZE, leftSharedContext);
                leftEndValuesFillContext = leftEndValues.makeFillContext(CHUNK_SIZE, leftSharedContext);
                leftStartValuesChunk = valueChunkType.makeWritableChunk(CHUNK_SIZE);
                leftEndValuesChunk = valueChunkType.makeWritableChunk(CHUNK_SIZE);
                leftValidity = WritableBooleanChunk.makeWritableChunk(CHUNK_SIZE);
                leftChunkPositions = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
                leftSortKernel = IntSortKernel.makeContext(valueChunkType, SortingOrder.Ascending, CHUNK_SIZE, true);

                // Final right resources
                rightFindRunsKernel = FindRunsKernel.makeContext(valueChunkType);

                // Resizable right resources
                ensureRightCapacity(CHUNK_SIZE);

                // Output resources
                outputSlotsFillFromContext = outputRedirection == null
                        ? outputSlotsExposed.makeFillFromContext(CHUNK_SIZE)
                        : outputSlotsInner.makeFillFromContext(CHUNK_SIZE);
                outputSlotsChunk = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
                outputStartPositionsInclusiveFillFromContext = outputRedirection == null
                        ? outputStartPositionsInclusiveExposed.makeFillFromContext(CHUNK_SIZE)
                        : outputStartPositionsInclusiveInner.makeFillFromContext(CHUNK_SIZE);
                outputStartPositionsInclusiveChunk = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
                outputEndPositionsExclusiveFillFromContext = outputRedirection == null
                        ? outputEndPositionsExclusiveExposed.makeFillFromContext(CHUNK_SIZE)
                        : outputEndPositionsExclusiveInner.makeFillFromContext(CHUNK_SIZE);
                outputEndPositionsExclusiveChunk = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            }

            private void ensureRightCapacity(final long rightGroupSize) {
                if (rightGroupSize > MAX_ARRAY_SIZE) {
                    throw new IllegalArgumentException(
                            String.format("%s: Unable to process right table bucket larger than %d, encountered %d",
                                    description, MAX_ARRAY_SIZE, rightGroupSize));
                }
                if (rightGroupSize == CLOSED_SENTINEL) {
                    throw new IllegalStateException(String.format("%s: used %s after close",
                            description, this.getClass()));
                }
                if (rightGroupSize > rightChunkSize) {
                    if (rightChunkSize > 0) {
                        rightChunkSize = 0; // Record that we don't want to re-close
                        final SafeCloseable sc1 = rightRangeValuesFillContext;
                        rightRangeValuesFillContext = null;
                        final SafeCloseable sc2 = rightRangeValuesChunk;
                        rightRangeValuesChunk = null;
                        final SafeCloseable sc3 = rightStartOffsets;
                        rightStartOffsets = null;
                        final SafeCloseable sc4 = rightLengths;
                        rightLengths = null;
                        SafeCloseable.closeAll(sc1, sc2, sc3, sc4);
                    }
                    rightChunkSize = (int) Math.min(MAX_ARRAY_SIZE, 1L << MathUtil.ceilLog2(rightGroupSize));
                    rightRangeValuesFillContext = rightRangeValues.makeFillContext(rightChunkSize);
                    rightRangeValuesChunk = valueChunkType.makeWritableChunk(rightChunkSize);
                    rightStartOffsets = WritableIntChunk.makeWritableChunk(rightChunkSize);
                    rightLengths = WritableIntChunk.makeWritableChunk(rightChunkSize);
                }
            }

            @Override
            public void close() {
                rightChunkSize = CLOSED_SENTINEL;
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
                        // Final right resources
                        rightFindRunsKernel,
                        // Resizable right resources
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

            final RowSet rightRows = rightGroupRowSets.get(index);
            final boolean nonEmptyRight = rightRows != null && rightRows.isNonempty();
            if (nonEmptyRight) {
                // Read right rows
                tc.ensureRightCapacity(rightRows.intSize());
                rightRangeValues.fillChunk(tc.rightRangeValuesFillContext, tc.rightRangeValuesChunk, rightRows);

                // Find and compact right runs, verifying order
                tc.rightFindRunsKernel.findRuns(tc.rightRangeValuesChunk, tc.rightStartOffsets, tc.rightLengths);
                final int firstOutOfOrderRightPosition = tc.rightFindRunsKernel.compactRuns(
                        tc.rightRangeValuesChunk, tc.rightStartOffsets);
                if (firstOutOfOrderRightPosition != -1) {
                    throw new OutOfOrderException(String.format(
                            "%s: Encountered out of order data in right table at row key %d",
                            description, rightRows.get(firstOutOfOrderRightPosition)));
                }
            }

            try (final RowSequence.Iterator leftRowsIterator = leftRows.getRowSequenceIterator()) {
                while (leftRowsIterator.hasMore()) {
                    final RowSequence leftRowsSlice = leftRowsIterator.getNextRowSequenceWithLength(CHUNK_SIZE);
                    final int sliceSize = leftRowsSlice.intSize();
                    leftStartValues.fillChunk(tc.leftStartValuesFillContext, tc.leftStartValuesChunk, leftRowsSlice);
                    leftEndValues.fillChunk(tc.leftEndValuesFillContext, tc.leftEndValuesChunk, leftRowsSlice);
                    tc.leftSharedContext.reset();

                    if (nonEmptyRight) {
                        rangeSearchKernel.populateInvalidRanges(
                                tc.leftStartValuesChunk, tc.leftEndValuesChunk, tc.leftValidity,
                                tc.outputStartPositionsInclusiveChunk, tc.outputEndPositionsExclusiveChunk);
                        valueChunkCompactKernel.compact(tc.leftStartValuesChunk, tc.leftValidity);
                        valueChunkCompactKernel.compact(tc.leftEndValuesChunk, tc.leftValidity);

                        tc.leftChunkPositions.setSize(tc.leftStartValuesChunk.size());
                        ChunkUtils.fillInOrder(tc.leftChunkPositions);
                        tc.leftSortKernel.sort(tc.leftChunkPositions, tc.leftStartValuesChunk);
                        rangeSearchKernel.findStarts(tc.leftStartValuesChunk, tc.leftChunkPositions,
                                tc.rightRangeValuesChunk, tc.rightStartOffsets, tc.rightLengths,
                                tc.outputStartPositionsInclusiveChunk);

                        tc.leftChunkPositions.setSize(tc.leftEndValuesChunk.size());
                        ChunkUtils.fillInOrder(tc.leftChunkPositions);
                        tc.leftSortKernel.sort(tc.leftChunkPositions, tc.leftEndValuesChunk);
                        rangeSearchKernel.findEnds(tc.leftEndValuesChunk, tc.leftChunkPositions,
                                tc.rightRangeValuesChunk, tc.rightStartOffsets, tc.rightLengths,
                                tc.outputEndPositionsExclusiveChunk);
                    } else {
                        rangeSearchKernel.populateAllRangeForEmptyRight(
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
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            super(jobScheduler, resultFuture);
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
