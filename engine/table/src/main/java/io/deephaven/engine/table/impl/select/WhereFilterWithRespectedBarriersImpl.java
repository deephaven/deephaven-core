//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import java.util.Arrays;

/**
 * A wrapper for a {@link WhereFilter} that declares this filter as respecting one or more barriers. A barrier is a
 * filter that must be executed before other filters that respect it. It is an error to respect a non-declared barrier.
 */
public class WhereFilterWithRespectedBarriersImpl extends WhereFilterDelegatingBase {
    /**
     * Wraps the provided {@link WhereFilter} with respecting-barrier behavior.
     *
     * @param filter the filter to wrap
     * @param respectedBarriers the barriers that this filter respects
     * @return a new {@code WhereFilter} instance that respects the barriers
     */
    public static WhereFilter of(WhereFilter filter, Object... respectedBarriers) {
        return new WhereFilterWithRespectedBarriersImpl(filter, respectedBarriers);
    }

    private final Object[] respectedBarriers;

    private WhereFilterWithRespectedBarriersImpl(
            WhereFilter filter,
            Object... respectedBarriers) {
        super(filter);
        this.respectedBarriers = respectedBarriers;
    }

    public Object[] respectedBarriers() {
        return respectedBarriers;
    }

    public WhereFilter copy() {
        return new WhereFilterWithRespectedBarriersImpl(filter.copy(), respectedBarriers);
    }

    @Override
    public String toString() {
        return "respectedBarrier{" +
                "respectedBarriers=" + Arrays.toString(respectedBarriers) +
                ", filter=" + filter +
                '}';
    }
}
