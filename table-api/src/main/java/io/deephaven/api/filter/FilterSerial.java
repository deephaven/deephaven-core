//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.filter;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * An immutable wrapper that enforces serial concurrency control on a filter.
 * <p>
 * FilterSerial wraps an underlying filter and ensures that it will not be executed in parallel by overriding
 * parallelization checks to always return {@code false}. Methods that return a Filter delegate to the wrapped filter
 * and rewrap the result as a {@code FilterSerial}, thus preserving the serial guarantee throughout filter
 * transformations.
 */
@Immutable
@SimpleStyle
public abstract class FilterSerial extends FilterBase implements Filter {

    /**
     * The underlying filter being wrapped.
     *
     * @return the inner filter
     */
    @Parameter
    public abstract Filter filter();

    /**
     * Creates a new FilterSerial wrapper for the given filter.
     *
     * @param filter the filter to wrap
     * @return a new instance of FilterSerial
     */
    public static FilterSerial of(Filter filter) {
        if (filter instanceof FilterSerial) {
            return (FilterSerial) filter;
        }

        return ImmutableFilterSerial.of(filter);
    }

    /**
     * Delegates the inversion to the wrapped filter and wraps the result in a FilterSerial.
     *
     * @return the inverted filter wrapped as a FilterSerial
     */
    @Override
    public Filter invert() {
        return FilterSerial.of(filter().invert());
    }

    /**
     * Applies serial concurrency control. Since this filter is already a serial wrapper, it returns itself.
     *
     * @return this instance, representing that serial control is already applied
     */
    @Override
    public Filter withSerial() {
        return this;
    }

    @Override
    public <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
