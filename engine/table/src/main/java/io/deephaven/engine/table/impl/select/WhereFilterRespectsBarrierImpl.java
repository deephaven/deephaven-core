//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.Strings;

import java.util.Arrays;

/**
 * A wrapper for a {@link WhereFilter} that declares this filter as respecting one or more barriers. A barrier is a
 * filter that must be executed before other filters that respect it. It is an error to respect a non-declared barrier.
 */
public class WhereFilterRespectsBarrierImpl extends WhereFilterDelegatingBase {
    /**
     * Wraps the provided {@link WhereFilter} with respecting-barrier behavior.
     *
     * @param filter the filter to wrap
     * @param respectedBarriers the barriers that this filter respects
     * @return a new {@code WhereFilterRespectsBarrierImpl} instance that respects the barriers
     */
    public static WhereFilter of(WhereFilter filter, Object... respectedBarriers) {
        return new WhereFilterRespectsBarrierImpl(filter, respectedBarriers);
    }

    private final Object[] respectedBarriers;

    private WhereFilterRespectsBarrierImpl(
            WhereFilter filter,
            Object... respectedBarriers) {
        super(filter);
        this.respectedBarriers = respectedBarriers;
    }

    public Object[] respectedBarriers() {
        return respectedBarriers;
    }

    public WhereFilter copy() {
        return new WhereFilterRespectsBarrierImpl(filter.copy(), respectedBarriers);
    }

    @Override
    public String toString() {
        return "respectsBarrier{" +
                "respectedBarriers=" + Arrays.toString(respectedBarriers) +
                ", filter=" + filter +
                '}';
    }
}
