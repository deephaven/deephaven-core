package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.filter.Filter;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.exceptions.OperationException;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerSparseArraySource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.table.impl.util.JobScheduler.IterateAction;
import io.deephaven.engine.table.impl.util.JobScheduler.JobThreadContext;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.deephaven.engine.table.impl.by.AggregationProcessor.EXPOSED_GROUP_ROW_SETS;

/**
 * Implementation for {@link QueryTable#rangeJoin(Table, Collection, RangeJoinMatch, Collection)}.
 */
public class RangeJoinOperation implements QueryTable.MemoizableOperation<QueryTable> {

    private static final ColumnName LEFT_ROW_SET = EXPOSED_GROUP_ROW_SETS;
    private static final ColumnName RIGHT_ROW_SET = ColumnName.of("__RIGHT_ROW_SET__");

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
                    "rangeJoin only supports static (not refreshing) inputs at this time: left table is %s, right table is %s",
                    leftTable.isRefreshing() ? "refreshing" : "static",
                    rightTable.isRefreshing() ? "refreshing" : "static"));
        }
        validateExactMatchColumns();
        validateRangeMatchColumns();
        SupportedRangeJoinAggregations.validate(aggregations);
    }

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
            throw new IllegalArgumentException(
                    String.format("Invalid exact matches: %s", String.join(", ", issues)));
        }
    }

    private void validateRangeMatchColumns() {
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
                    "Invalid range match %s: %s", Strings.of(rangeMatch), String.join(", ", issues)));
        }
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
            outputRightTableGrouped = exposeGroupRowSets(
                    ((QueryTable) rightTable.coalesce().where(Filter.isNotNull(rangeMatch.rightRangeColumn()))),
                    JoinMatch.rights(exactMatches));
        }
    }

    private static QueryTable exposeGroupRowSets(
            @NotNull final QueryTable inputTable,
            @NotNull final Collection<ColumnName> exactMatches) {
        return inputTable.aggNoMemo(AggregationProcessor.forExposeGroupRowSets(), false, null, exactMatches);
    }

    private class StaticRangeJoinPhase2 extends RangeJoinPhase implements IterateAction<JobThreadContext> {

        private final RowRedirection outputRedirection;
        private final WritableColumnSource<Integer> outputSlotsAndPositionRanges;

        private ColumnSource<RowSet> leftGroupRowSets;
        private ColumnSource<RowSet> rightGroupRowSets;

        private StaticRangeJoinPhase2(
                @NotNull final JobScheduler jobScheduler,
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            super(jobScheduler, resultFuture);
            if (!leftTable.isFlat() && SparseConstants.sparseStructureExceedsOverhead(
                    leftTable.getRowSet(), MAXIMUM_STATIC_MEMORY_OVERHEAD)) {
                outputRedirection = new MultiplierWritableRowRedirection(
                        new InverseWrappedRowSetRowRedirection(leftTable.getRowSet()), 3);
                outputSlotsAndPositionRanges = WritableRedirectedColumnSource.maybeRedirect(
                        outputRedirection,
                        InMemoryColumnSource.getImmutableMemoryColumnSource(leftTable.size(), int.class, null),
                        leftTable.size() * 3);
            } else {
                outputRedirection = null;
                outputSlotsAndPositionRanges = new IntegerSparseArraySource();
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
                    JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0,
                    joinedInputTables.intSize(),
                    this,
                    () -> new StaticRangeJoinPhase3(jobScheduler, resultFuture).start(outputSlotsAndPositionRanges),
                    resultFuture::completeExceptionally);
        }

        private class TaskContext implements JobScheduler.JobThreadContext {

//            private final SizedLongChunk<RowKeys>
        }

        @Override
        public void run(
                @NotNull final JobThreadContext taskThreadContext,
                final int index,
                @NotNull final Consumer<Exception> nestedErrorConsumer) {
            // TODO-RWC: Grab groups, do stamping, update outputSlotsAndPositionRanges
            final RowSet leftRows = leftGroupRowSets.get(index);
            final RowSet rightRows = rightGroupRowSets.get(index);

        }
    }

    private class StaticRangeJoinPhase3 extends RangeJoinPhase {

        private StaticRangeJoinPhase3(
                @NotNull final JobScheduler jobScheduler,
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            super(jobScheduler, resultFuture);
        }

        public void start(@NotNull final ColumnSource<Integer> outputSlotsAndPositionRanges) {
            // TODO-RWC: Build grouped sources, return result QueryTable
        }
    }
}
