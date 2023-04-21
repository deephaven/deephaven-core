package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.exceptions.OperationException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.WritableColumnSource;
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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
        if (leftTable.isRefreshing() || rightTable.isRefreshing()) {
            throw new UnsupportedOperationException(String.format(
                    "rangeJoin only supports static (not refreshing) inputs at this time: left table is %s, right table is %s",
                    leftTable.isRefreshing() ? "refreshing" : "static",
                    rightTable.isRefreshing() ? "refreshing" : "static"));
        }

        SupportedRangeJoinAggregations.validate(aggregations);

        QueryTable.checkInitiateBinaryOperation(leftTable, rightTable);

        return new Result<>(staticRangeJoin());
    }

    @Override
    public MemoizedOperationKey getMemoizedOperationKey() {
        return memoizedOperationKey;
    }

    private QueryTable staticRangeJoin() {

        final JobScheduler jobScheduler;
        if (OperationInitializationThreadPool.NUM_THREADS > 1
                && !OperationInitializationThreadPool.isInitializationThread()) {
            jobScheduler = new OperationInitializationPoolJobScheduler();
        } else {
            jobScheduler = ImmediateJobScheduler.INSTANCE;
        }

        final CompletableFuture<QueryTable> resultFuture = new CompletableFuture<>();
        new StaticRangeJoinPhase1(jobScheduler, resultFuture).start();
        try {
            return resultFuture.get();
        } catch (InterruptedException e) {
            throw new CancellationException(String.format("%s interrupted", description), e);
        } catch (ExecutionException e) {
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

    private class StaticRangeJoinPhase1 extends RangeJoinPhase implements IterateAction<JobThreadContext> {

        private static final int LEFT_TASK_INDEX = 0;
        private static final int RIGHT_TASK_INDEX = 1;
        private static final int START_TASK_INDEX = LEFT_TASK_INDEX;
        private static final int TASK_COUNT = RIGHT_TASK_INDEX + 1;

        private Table outputLeftTableGrouped;
        private Table outputRightTableGrouped;

        private StaticRangeJoinPhase1(
                @NotNull final JobScheduler jobScheduler,
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            super(jobScheduler, resultFuture);
        }

        private void start() {
            jobScheduler.iterateParallel(
                    ExecutionContext.getContextToRecord(),
                    logOutput -> logOutput.append("range join input grouping"),
                    JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    START_TASK_INDEX,
                    TASK_COUNT,
                    this,
                    () -> new StaticRangeJoinPhase2(jobScheduler, resultFuture)
                            .start(outputLeftTableGrouped, outputRightTableGrouped),
                    resultFuture::completeExceptionally);
        }

        @Override
        public void run(
                @NotNull final JobThreadContext taskThreadContext,
                final int index,
                @NotNull final Consumer<Exception> nestedErrorConsumer) {
            switch (index) {
                case LEFT_TASK_INDEX:
                    outputLeftTableGrouped = exposeGroupRowSets(
                            leftTable,
                            JoinMatch.lefts(exactMatches));
                    break;
                case RIGHT_TASK_INDEX:
                    outputRightTableGrouped = exposeGroupRowSets(
                            ((QueryTable) rightTable.coalesce().where(Filter.isNotNull(rangeMatch.rightRangeColumn()))),
                            JoinMatch.rights(exactMatches));
                    break;
                default:
                    // noinspection ThrowableNotThrown
                    Assert.statementNeverExecuted(String.format("Unexpected task index %d", index));
            }
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

        private Table joinedInputTables;

        private StaticRangeJoinPhase2(
                @NotNull final JobScheduler jobScheduler,
                @NotNull final CompletableFuture<QueryTable> resultFuture) {
            super(jobScheduler, resultFuture);
            if (!leftTable.isFlat() && SparseConstants.sparseStructureExceedsOverhead(
                    leftTable.getRowSet(), MAXIMUM_STATIC_MEMORY_OVERHEAD)) {
                outputRedirection = new MultiplierWritableRowRedirection(
                        new InverseWrappedRowSetWritableRowRedirection(leftTable.getRowSet()), 3);
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
            joinedInputTables = leftTableGrouped.naturalJoin(
                    rightTableGrouped, exactMatches, List.of(JoinAddition.of(RIGHT_ROW_SET, EXPOSED_GROUP_ROW_SETS)));

        }

        @Override
        public void run(
                @NotNull final JobThreadContext taskThreadContext,
                final int index,
                @NotNull final Consumer<Exception> nestedErrorConsumer) {

        }
    }
}
