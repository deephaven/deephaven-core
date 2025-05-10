//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

public interface PushdownPredicateManager {
    /**
     * Push down the given filter to the underlying table and return the result.
     *
     * @param columnSourceMap The map of column sources to use for the filter.
     * @param filter The {@link Filter filter} to apply.
     * @param input The set of rows to test.
     * @param fullSet The full set of rows
     * @param usePrev Whether to use the previous result
     * @param context The {@link FilterContext} to use for the pushdown operation.
     * @param costCeiling Execute all possible filters with a cost leq this value.
     * @param jobScheduler The job scheduler to use for scheduling child jobs
     * @param onComplete Consumer of the output rowsets for added and modified rows that pass the filter
     * @param onError Consumer of any exceptions that occur during the pushdown operation
     */
    default void pushdownFilter(
            final Map<String, ColumnSource<?>> columnSourceMap,
            final WhereFilter filter,
            final RowSet input,
            final RowSet fullSet,
            final boolean usePrev,
            final FilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError) {
        // Default to returning all results as maybe
        onComplete.accept(PushdownResult.of(RowSetFactory.empty(), input.copy()));
    }

    /**
     * Estimate the cost of pushing down a filter. This returns a unitless value that can be used to compare the cost of
     * executing different filters.
     *
     * @param filter The {@link Filter filter} to test.
     * @param selection The set of rows to tests.
     * @param fullSet The full set of rows
     * @param usePrev Whether to use the previous result
     * @param context The {@link FilterContext} to use for the pushdown operation.
     * @return The estimated cost of the push down operation.
     */
    default long estimatePushdownFilterCost(
            WhereFilter filter,
            RowSet selection,
            RowSet fullSet,
            boolean usePrev,
            FilterContext context) {
        return Long.MAX_VALUE; // No benefit to pushing down.
    }

    /**
     * Make a filter context for this pushdown predicate manager. This is used to pass the filter and other information
     * to the filtering code.
     *
     * @param filter the filter that belongs to this context
     * @return the created filter context
     */
    default FilterContext makeFilterContext(final WhereFilter filter) {
        return FilterContext.of(filter, this);
    }

    /**
     * Return the shared pushdown predicate manager for the given column sources, if one exists. Otherwise, return null.
     * 
     * @param columnSources The column sources to check.
     * @return The shared pushdown predicate manager, or null if none exists.
     */
    static PushdownPredicateManager getPushdownPredicateManager(Collection<ColumnSource<?>> columnSources) {
        // If all column sources are AbstractColumnSource and share the same pushdown predicate manager, return it.
        final ColumnSource<?> first = columnSources.iterator().next();
        if (first instanceof AbstractColumnSource && ((AbstractColumnSource) first).pushdownManager() != null) {
            final PushdownPredicateManager manager = ((AbstractColumnSource) first).pushdownManager();
            if (columnSources.stream().allMatch(cs -> cs instanceof AbstractColumnSource
                    && ((AbstractColumnSource) cs).pushdownManager() == manager)) {
                return manager;
            }
        }
        return null;
    }
}
