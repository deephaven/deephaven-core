//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;

import java.util.function.Consumer;

public interface PushdownFilterMatcher {
    /**
     * Estimate the cost of pushing down the next pushdownd filter. This returns a unitless value that can be used to
     * compare the cost of executing different filters.
     *
     * @param filter The {@link Filter filter} to test.
     * @param selection The set of rows to tests.
     * @param fullSet The full set of rows
     * @param usePrev Whether to use the previous result
     * @param context The {@link PushdownFilterContext} to use for the pushdown operation.
     * @return The estimated cost of the push down operation.
     */
    long estimatePushdownFilterCost(
            final WhereFilter filter,
            final RowSet selection,
            final RowSet fullSet,
            final boolean usePrev,
            final PushdownFilterContext context);

    /**
     * Push down the given filter to the underlying table and pass the result to the consumer. This method is expected
     * to execute all pushdown filters that are <= the cost ceiling and to call
     * {@link PushdownFilterContext#updateExecutedFilterCost(long)}
     *
     * @param filter The {@link Filter filter} to apply.
     * @param selection The set of rows to test.
     * @param fullSet The full set of rows
     * @param usePrev Whether to use the previous result
     * @param context The {@link PushdownFilterContext} to use for the pushdown operation.
     * @param costCeiling Execute all possible filters with a cost <= this value.
     * @param jobScheduler The job scheduler to use for scheduling child jobs
     * @param onComplete Consumer of the output rowsets for added and modified rows that pass the filter
     * @param onError Consumer of any exceptions that occur during the pushdown operation
     */
    void pushdownFilter(
            final WhereFilter filter,
            final RowSet selection,
            final RowSet fullSet,
            final boolean usePrev,
            final PushdownFilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError);

    /**
     * Make a filter context for this column source that does not support pushdown filtering. This should be overriden
     * for column sources that do support pushdown filtering.
     *
     * @return the created filter context
     */
    PushdownFilterContext makePushdownFilterContext();
}
