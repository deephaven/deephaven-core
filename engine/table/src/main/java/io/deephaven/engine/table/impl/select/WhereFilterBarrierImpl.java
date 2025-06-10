//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

/**
 * A wrapper for a {@link WhereFilter} that declares this filter as a barrier. A barrier is a filter that must be
 * executed before other filters that respect it.
 */
public class WhereFilterBarrierImpl extends WhereFilterDelegatingBase {
    /**
     * Wraps the provided {@link WhereFilter} with a barrier declaration.
     *
     * @param filter the filter to wrap
     * @param barrier the barrier that this filter declares
     * @return a new {@code WhereFilterBarrierImpl} instance that declares the barrier
     */
    public static WhereFilter of(WhereFilter filter, Object barrier) {
        return new WhereFilterBarrierImpl(filter, barrier);
    }

    private final Object barrier;

    private WhereFilterBarrierImpl(
            WhereFilter filter,
            Object barrier) {
        super(filter);
        this.barrier = barrier;
    }

    public Object barrier() {
        return barrier;
    }

    public WhereFilter copy() {
        return new WhereFilterBarrierImpl(filter.copy(), barrier);
    }

    @Override
    public String toString() {
        return "barrier{" + barrier + ", filter=" + filter + "}";
    }
}
