//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.SafeCloseable;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Helper class to manage filters and their associated execution cost.
 */
public class FilterContext implements SafeCloseable {
    private final WhereFilter filter;
    private final PushdownPredicateManager ppm;
    private final AbstractColumnSource<?> columnSource;

    // Store results of the push-down operation.
    private PushdownResult pushdownResult = null;

    // As we execute potentially many push down filters, store the most recent (and maximum) cost experienced so far.
    private long executedFilterCost;
    // The cost of executing the next pushdown filter step.
    private long pushdownFilterCost;

    protected FilterContext(
            WhereFilter filter,
            PushdownPredicateManager ppm,
            AbstractColumnSource<?> columnSource) {
        this.filter = filter;
        this.ppm = ppm;
        this.columnSource = columnSource;
        executedFilterCost = 0;
        pushdownFilterCost = Long.MAX_VALUE;
    }

    public static FilterContext of(WhereFilter filter, PushdownPredicateManager ppm) {
        return new FilterContext(filter, ppm, null);
    }

    public static FilterContext of(WhereFilter filter, AbstractColumnSource<?> columnSource) {
        return new FilterContext(filter, null, columnSource);
    }

    public static FilterContext of(WhereFilter filter) {
        return new FilterContext(filter, null, null);
    }

    public WhereFilter filter() {
        return filter;
    }

    public PushdownResult pushdownResult() {
        return pushdownResult;
    }

    public void updatePushdownResult(final PushdownResult result) {
        if (pushdownResult != null) {
            pushdownResult.close();
        }
        this.pushdownResult = result;
    }

    public PushdownPredicateManager ppm() {
        return ppm;
    }

    public AbstractColumnSource<?> columnSource() {
        return columnSource;
    }

    public long executedFilterCost() {
        return executedFilterCost;
    }

    public void updateExecutedFilterCost(final long cost) {
        executedFilterCost = cost;
    }

    public long pushdownFilterCost() {
        return pushdownFilterCost;
    }

    private void computeCosts(
            final Table sourceTable,
            final RowSet input,
            final boolean usePrev) {

        if (filter.getColumns().isEmpty()) {
            // With no input columns, a stateless filter must be effectively constant output and low cost.
            pushdownFilterCost = Long.MAX_VALUE;
        } else if (filter.getColumns().size() == 1) {
            if (columnSource == null) {
                // This a non-standard column source, assume it is expensive to filter with no push-down.
                pushdownFilterCost = Long.MAX_VALUE;
                return;
            }
            pushdownFilterCost =
                    columnSource.estimatePushdownFilterCost(filter, input, sourceTable.getRowSet(), usePrev, this);
        } else {
            if (ppm != null) {
                pushdownFilterCost =
                        ppm.estimatePushdownFilterCost(filter, input, sourceTable.getRowSet(), usePrev, this);
            } else {
                pushdownFilterCost = Long.MAX_VALUE;
            }
        }
    }

    /**
     * Create a list of FilterContext objects for the given filters and the supplied input. This will create at least
     * one entry for each filter, and may create a second entry if the filter can be efficiently pushed down.
     */
    public static void sortFilterContexts(
            final int startIndex,
            final FilterContext[] filterContexts,
            final Table sourceTable,
            final RowSet input,
            final boolean usePrev) {

        // Compute (or recompute) the costs for each filter in the list, starting at the given index.
        for (int i = startIndex; i < filterContexts.length; i++) {
            final FilterContext frc = filterContexts[i];
            frc.computeCosts(sourceTable, input, usePrev);
        }

        // Sort the filters by non-descending cost, starting at the given index.
        Arrays.sort(filterContexts, startIndex, filterContexts.length,
                Comparator.comparingLong(fc -> fc.pushdownFilterCost));
    }

    @Override
    public void close() {
        if (pushdownResult != null) {
            pushdownResult.close();
            pushdownResult = null;
        }
    }
}
