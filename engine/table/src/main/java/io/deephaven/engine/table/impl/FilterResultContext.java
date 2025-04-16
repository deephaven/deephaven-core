//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import com.google.common.math.LongMath;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.SafeCloseable;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Helper class to manage filters and their associated execution cost.
 */
public final class FilterResultContext implements SafeCloseable {
    private final WhereFilter filter;
    private final PushdownPredicateManager ppm;

    // Cost may change as the filter input changes and Long.MAX_VALUE means no pushdown should be attempted.
    private long pushdownCost;
    private long filterCost;

    // When this filter is being evaluated, should we consider push-down?
    private boolean pushdownNeeded = true;

    // Store results of the push-down operation.
    private PushdownResult pushdownResult = null;

    public FilterResultContext(
            WhereFilter filter,
            PushdownPredicateManager ppm) {
        this.filter = filter;
        this.filterCost = Long.MAX_VALUE;
        this.pushdownCost = Long.MAX_VALUE;
        this.ppm = ppm;
    }

    public WhereFilter filter() {
        return filter;
    }

    public boolean pushdownNeeded() {
        return pushdownNeeded;
    }

    public void markPushdownComplete() {
        this.pushdownNeeded = false;
    }

    public PushdownResult pushdownResult() {
        return pushdownResult;
    }

    public void setPushdownResult(final PushdownResult result) {
        if (pushdownResult != null) {
            pushdownResult.close();
        }
        this.pushdownResult = result;
    }

    public PushdownPredicateManager ppm() {
        return ppm;
    }

    private void computeCosts(
            final Table sourceTable,
            final RowSet input,
            final boolean usePrev) {

        if (filter.getColumns().isEmpty()) {
            // With no input columns, a stateless filter must be effectively constant output and low cost.
            filterCost = 0;
            // Won't consider pushdown for stateless filters.
            pushdownCost = Long.MAX_VALUE;
            pushdownNeeded = false;
        } else if (filter.getColumns().size() == 1) {
            // Get the column source and estimate the cost of applying the filter.
            final AbstractColumnSource<?> acs =
                    (AbstractColumnSource<?>) sourceTable.getColumnSource(filter.getColumns().get(0));
            filterCost = acs.estimateFilterCost(filter, input, sourceTable.getRowSet(), usePrev);
            if (pushdownNeeded) {
                pushdownCost = acs.estimatePushdownFilterCost(filter, input, sourceTable.getRowSet(), usePrev);
                pushdownNeeded = pushdownCost < filterCost;
            }
        } else {
            // Estimate the filter cost by summing the costs of each column source.
            filterCost = 0;
            for (final String column : filter.getColumns()) {
                final AbstractColumnSource<?> acs = (AbstractColumnSource<?>) sourceTable.getColumnSource(column);
                filterCost = LongMath.saturatedAdd(filterCost,
                        acs.estimateFilterCost(filter, input, sourceTable.getRowSet(), usePrev));
            }

            if (pushdownNeeded) {
                pushdownCost = ppm != null
                        ? ppm.estimatePushdownFilterCost(filter, input, sourceTable.getRowSet(), usePrev)
                        : Long.MAX_VALUE;
                pushdownNeeded = pushdownCost < filterCost;
            }
        }
    }

    /**
     * Create a list of FilterResultContext objects for the given filters and the supplied input. This will create at
     * least one entry for each filter, and may create a second entry if the filter can be efficiently pushed down.
     */
    public static void sortFilterContexts(
            final int startIndex,
            final FilterResultContext[] filterResultContexts,
            final Table sourceTable,
            final RowSet input,
            final boolean usePrev) {

        // Compute (or recompute) the costs for each filter in the list, starting at the given index.
        for (int i = startIndex; i < filterResultContexts.length; i++) {
            final FilterResultContext frc = filterResultContexts[i];
            frc.computeCosts(sourceTable, input, usePrev);
        }

        // Sort the filters by non-descending cost, starting at the given index.
        Arrays.sort(filterResultContexts, startIndex, filterResultContexts.length,
                Comparator.comparingLong(frc -> Math.min(frc.pushdownCost, frc.filterCost)));
    }

    @Override
    public void close() {
        if (pushdownResult != null) {
            pushdownResult.close();
            pushdownResult = null;
        }
    }
}
