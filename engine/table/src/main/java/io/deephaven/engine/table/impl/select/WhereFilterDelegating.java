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
     * Returns the wrapped filter if and only if the wrapped filter accepts the same rows as this filter. For example,
     * serial and barrier wrappers do not change functionality and will return the wrapped filter. Filters such as
     * {@link WhereFilterInvertedImpl} that perform a transformation should return themselves.
     */
    WhereFilter maybeUnwrapFilter();

    /**
     * If the provided filter is an instance of {@code WhereFilterDelegating}, returns the effective wrapped filter of
     * that filter. Otherwise, returns the filter itself.
     */
    static WhereFilter maybeUnwrapFilter(WhereFilter filter) {
        if (filter instanceof WhereFilterDelegating) {
            return ((WhereFilterDelegating) filter).maybeUnwrapFilter();
        }
        // This filter is not a delegating filter, so return it directly.
        return filter;
    }
}
