//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.filter;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * An immutable wrapper that declares a concurrency control barrier on a filter.
 * <p>
 * FilterBarrier wraps an underlying filter and declares a barrier that other filters can respect. Respecting a barrier
 * ensures that filters are not reordered with respect to the barrier. This is useful to ensure that stateful filters
 * process a predictable and well-defined set of rows.
 */
@Immutable
@SimpleStyle
public abstract class FilterBarrier extends FilterBase implements Filter {

    /**
     * The underlying filter being wrapped.
     *
     * @return the inner filter
     */
    @Parameter
    public abstract Filter filter();

    /**
     * The barrier object that can be used to synchronize or coordinate with other filters.
     *
     * @return the barrier object
     */
    @Parameter
    public abstract Object barrier();

    /**
     * Creates a new FilterBarrier wrapper for the given filter and barrier.
     *
     * @param filter the filter to wrap
     * @param barrier the barrier object being declared
     * @return a new instance of FilterBarrier
     */
    public static FilterBarrier of(Filter filter, Object barrier) {
        return ImmutableFilterBarrier.of(filter, barrier);
    }

    /**
     * Delegates the inversion to the wrapped filter and wraps the result in a FilterBarrier.
     *
     * @return the inverted filter wrapped as a FilterBarrier
     */
    @Override
    public Filter invert() {
        return FilterBarrier.of(filter().invert(), barrier());
    }

    @Override
    public <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
