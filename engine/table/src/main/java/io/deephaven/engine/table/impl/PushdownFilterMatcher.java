//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Interface for entities that support pushdown filtering. Must implement a filter cost estimation function that allows
 * comparison of filter pushdown execution costs. These costs determine the order in which filters are executed.
 * <p>
 * NOTE: There may be multiple pushdown filter operations available for a single filter and the pushdown cost is dynamic
 * based on the input rowset and on previously executed pushdown filter steps. For example, parquet table locations may
 * leverage low cost metadata operations (row group min/max) as a first step, followed by an index table operation or a
 * binary search on a sorted column. The {@link PushdownFilterContext} is used to track the state of the pushdown filter
 * to return accurate cost estimates for the next step.
 */
public interface PushdownFilterMatcher {
    /**
     * Estimate the cost of pushing down the next pushdown filter. This returns a unitless value to compare the cost of
     * executing different filters. Common costs are listed in {@link PushdownResult} (such as
     * {@link PushdownResult#METADATA_STATS_COST}) and should be used as a baseline for estimating the cost of newly
     * implemented pushdown operations.
     *
     * <p>
     * A no-op implementation should simply complete with {@link Long#MAX_VALUE}.
     *
     * @param filter The {@link Filter filter} to test.
     * @param selection The set of rows to tests.
     * @param usePrev Whether to use the previous result
     * @param context The {@link PushdownFilterContext} to use for the pushdown operation.
     * @param jobScheduler The job scheduler to use for scheduling child jobs
     * @param onComplete Consumer of the estimated cost of the pushdown operation. {@value Long#MAX_VALUE} indicates
     *        that the filter cannot be pushed down.
     * @param onError Consumer of any exceptions that occur during the estimate operation
     */
    void estimatePushdownFilterCost(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final JobScheduler jobScheduler,
            final LongConsumer onComplete,
            final Consumer<Exception> onError);

    /**
     * Push down the given filter to the underlying table and pass the result to the consumer. This method is expected
     * to execute all pushdown filter steps that are greater than {@link PushdownFilterContext#executedFilterCost()} and
     * less than or equal to {@code costCeiling}.
     *
     * <p>
     * The resulting {@link PushdownResult} (to {@code onComplete}) must only contain rows from {@code selection}. The
     * {@link PushdownResult#match() match row set} are rows that are guaranteed to match. The implicitly "missing" rows
     * {@code selection - match - maybeMatch} are rows that are guaranteed to <b>not</b> match. The remaining
     * {@link PushdownResult#maybeMatch() maybe match row set} are rows that may, or may not, match. The pushdown result
     * ownership passes to {@code onComplete}.
     *
     * <p>
     * A no-op implementation should simply complete with {@code PushdownResult.maybeMatch(selection)}.
     *
     * @param filter The {@link Filter filter} to apply.
     * @param selection The set of rows to test.
     * @param usePrev Whether to use the previous result
     * @param context The {@link PushdownFilterContext} to use for the pushdown operation.
     * @param costCeiling Execute all possible filters with a cost less than or equal this value.
     * @param jobScheduler The job scheduler to use for scheduling child jobs
     * @param onComplete Consumer of the output rowsets for added and modified rows that pass the filter
     * @param onError Consumer of any exceptions that occur during the pushdown operation
     */
    void pushdownFilter(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError);

    /**
     * Create a pushdown filter context for this entity.
     *
     * @param filter the filter to use while making the context
     * @param filterSources the column sources that match the filter column names
     *
     * @return the created filter context
     */
    PushdownFilterContext makePushdownFilterContext(
            final WhereFilter filter,
            final List<ColumnSource<?>> filterSources);

    /**
     * Given a filter and a list of column sources, return the appropriate {@link PushdownFilterMatcher} to use for
     * pushing down the filter (or {@code null} when pushdown is not supported).
     *
     * @param filter The {@link WhereFilter filter} to match.
     * @param filterSources The list of {@link ColumnSource column sources} that match the filter columns.
     * @return The {@link PushdownFilterMatcher} to use for pushing down the filter, or {@code null} if pushdown is not
     *         supported.
     */
    static PushdownFilterMatcher getPushdownFilterMatcher(
            final WhereFilter filter,
            final List<ColumnSource<?>> filterSources) {
        // Select the executor to use for this filter (or assign null if pushdown is not supported).
        if (!PushdownFilterMatcher.canPushdownFilter(filter)) {
            return null;
        } else if (filter.getColumns().size() > 1) {
            return PushdownPredicateManager.getSharedPPM(filterSources);
        } else if (filter.getColumns().size() == 1) {
            final ColumnSource<?> columnSource = filterSources.get(0);
            return (columnSource instanceof AbstractColumnSource)
                    ? (AbstractColumnSource<?>) columnSource
                    : null;
        }
        return null;
    }

    /**
     * Check if the given filter can be pushed down.
     *
     * @param filter The {@link WhereFilter filter} to check.
     * @return {@code true} if the filter can be pushed down, {@code false} otherwise.
     */
    static boolean canPushdownFilter(final WhereFilter filter) {
        return !filter.getColumns().isEmpty()
                && !filter.hasVirtualRowVariables()
                && filter.getColumnArrays().isEmpty();
    }
}
