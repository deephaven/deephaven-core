/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Evaluates to {@code true} when the given {@link #filter() filter} evaluates to {@code false}.
 */
@Immutable
@SimpleStyle
public abstract class FilterNot<F extends Filter> extends FilterBase {

    public static <F extends Filter> FilterNot<F> of(F filter) {
        return ImmutableFilterNot.of(filter);
    }

    /**
     * The filter.
     *
     * @return the filter
     */
    @Parameter
    public abstract F filter();

    /**
     * Equivalent to {@code filter()}.
     *
     * @return the inverse filter
     */
    @Override
    public final F inverse() {
        return filter();
    }

    /**
     * Creates the logical equivalent of {@code this}. Equivalent to {@code filter().inverse()}. It's possible that the
     * "simplification" is equal to {@code this}.
     *
     * @return the simplified filter
     */
    public final Filter simplify() {
        return filter().inverse();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
