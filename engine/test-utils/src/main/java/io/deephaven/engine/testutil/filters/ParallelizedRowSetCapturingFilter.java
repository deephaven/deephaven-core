//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.filters;

import io.deephaven.api.filter.Filter;

/**
 * Helper to force parallelization of the RowSetCapturingFilter.
 */
public class ParallelizedRowSetCapturingFilter extends RowSetCapturingFilter {
    public ParallelizedRowSetCapturingFilter(Filter filter) {
        super(filter);
    }

    @Override
    public boolean permitParallelization() {
        return true;
    }
}
