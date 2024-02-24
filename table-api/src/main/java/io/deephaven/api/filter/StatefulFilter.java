/**
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value;

/**
 * A {@link Filter} that wraps another {@code Filter}, and is used to indicate that the wrapped filter has side effects
 * that prevent parallelization. (e.g. a filter that depends on processing rows in order, or would suffer from lock
 * contention if parallelized)
 */
@Value.Immutable
@SimpleStyle
public abstract class StatefulFilter implements Filter {
    public static StatefulFilter of(Filter innerFilter) {
        return ImmutableStatefulFilter.of(innerFilter);
    }

    @Value.Parameter
    public abstract Filter innerFilter();

    @Override
    public <T> T walk(Expression.Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Filter invert() {
        return StatefulFilter.of(innerFilter().invert());
    }

    @Override
    public <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
