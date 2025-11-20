//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.filters;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.select.WhereFilter;

/**
 * Helper to force parallelization of the RowSetCapturingFilter.
 */
public class ParallelizedRowSetCapturingFilter extends RowSetCapturingFilter {
    private ParallelizedRowSetCapturingFilter(WhereFilter filter,
            java.util.List<io.deephaven.engine.rowset.RowSet> rowSets) {
        super(filter, rowSets);
    }

    public ParallelizedRowSetCapturingFilter(Filter filter) {
        super(filter);
    }

    @Override
    public boolean permitParallelization() {
        return true;
    }

    @Override
    public WhereFilter copy() {
        if (innerFilter != null) {
            final WhereFilter newInner = innerFilter.copy();
            if (newInner != innerFilter) {
                // note we share the rowset collection
                return new ParallelizedRowSetCapturingFilter(newInner, rowSets);
            }
        }
        return this;
    }
}
