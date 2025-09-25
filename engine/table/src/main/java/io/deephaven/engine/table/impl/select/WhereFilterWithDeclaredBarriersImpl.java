//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import java.util.Arrays;

/**
 * A wrapper for a {@link WhereFilter} that declares a barrier on this filter. A barrier indicates that one filter must
 * be executed before other filters that respect it.
 */
public class WhereFilterWithDeclaredBarriersImpl extends WhereFilterDelegatingBase {
    /**
     * Wraps the provided {@link WhereFilter} with one or more barrier declarations.
     *
     * @param filter the filter to wrap
     * @param declaredBarriers the barrier objects that this filter declares
     * @return a new {@code WhereFilterBarrierImpl} instance that declares the barrier
     */
    public static WhereFilter of(WhereFilter filter, Object... declaredBarriers) {
        return new WhereFilterWithDeclaredBarriersImpl(filter, declaredBarriers);
    }

    private final Object[] declaredBarriers;

    private WhereFilterWithDeclaredBarriersImpl(
            WhereFilter filter,
            Object... declaredBarriers) {
        super(filter);
        this.declaredBarriers = declaredBarriers;
    }

    public Object[] declaredBarriers() {
        return declaredBarriers;
    }

    public WhereFilter copy() {
        return new WhereFilterWithDeclaredBarriersImpl(filter.copy(), declaredBarriers);
    }

    @Override
    public String toString() {
        return "withDeclaredBarriers{" + Arrays.toString(declaredBarriers) + ", filter=" + filter + "}";
    }
}
