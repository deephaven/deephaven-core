//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import java.util.Arrays;

/**
 * A wrapper for a {@link WhereFilter} that declares this filter as a barrier. A barrier is a filter that must be
 * executed before other filters that respect it.
 */
public class WhereFilterBarrierImpl extends WhereFilterDelegatingBase {
    /**
     * Wraps the provided {@link WhereFilter} with one or more barrier declarations.
     *
     * @param filter the filter to wrap
     * @param barriers the barrier objects that this filter declares
     * @return a new {@code WhereFilterBarrierImpl} instance that declares the barrier
     */
    public static WhereFilter of(WhereFilter filter, Object... barriers) {
        return new WhereFilterBarrierImpl(filter, barriers);
    }

    private final Object[] barriers;

    private WhereFilterBarrierImpl(
            WhereFilter filter,
            Object... barriers) {
        super(filter);
        this.barriers = barriers;
    }

    public Object[] barriers() {
        return barriers;
    }

    public WhereFilter copy() {
        return new WhereFilterBarrierImpl(filter.copy(), barriers);
    }

    @Override
    public String toString() {
        return "barrier{" + Arrays.toString(barriers) + ", filter=" + filter + "}";
    }
}
