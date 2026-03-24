//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

public interface WhereFilterDelegating {

    /**
     * Returns the filter to which this filter delegates.
     */
    WhereFilter getWrappedFilter();

    /**
     * Returns the wrapped filter if and only if the wrapped filter is functionally identical to this filter. Filters
     * that perform a transformation (e.g. {@code not}) on their wrapped filter should return themselves.
     */
    WhereFilter maybeUnwrapFilter();

    /**
     * If the provided filter is an instance of {@code WhereFilterDelegating}, returns the effective wrapped filter of
     * that filter. Otherwise, returns the filter itself wrapped in an Optional.
     */
    static WhereFilter maybeUnwrapFilter(WhereFilter filter) {
        if (filter instanceof WhereFilterDelegating) {
            return ((WhereFilterDelegating) filter).maybeUnwrapFilter();
        }
        // This filter is not a delegating filter, so return it directly.
        return filter;
    }
}
