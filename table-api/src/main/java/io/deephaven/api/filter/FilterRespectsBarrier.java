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
public abstract class FilterRespectsBarrier extends FilterBase implements Filter {

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
    public abstract Object[] respectedBarriers();

    /**
     * Creates a new FilterRespectsBarrier wrapper for the given filter and the provided barriers.
     *
     * @param filter the filter to wrap
     * @param barriers the barrier objects that must be respected
     * @return a new instance of FilterSerial
     */
    public static FilterRespectsBarrier of(Filter filter, Object... barriers) {
        return ImmutableFilterRespectsBarrier.of(filter, barriers);
    }

    /**
     * Delegates the inversion to the wrapped filter and wraps the result in a FilterBarrier.
     *
     * @return the inverted filter wrapped as a FilterBarrier
     */
    @Override
    public Filter invert() {
        return FilterRespectsBarrier.of(filter().invert(), respectedBarriers());
    }

    @Override
    public <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
