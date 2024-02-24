/**
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.expression;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value;

/**
 * A {@link Expression} that wraps another {@code Expression}, and is used to indicate that the wrapped filter has side
 * effects that prevent parallelization. (e.g. a filter that depends on processing rows in order, or would suffer from
 * lock contention if parallelized)
 */
@Value.Immutable
@SimpleStyle
public abstract class StatefulExpression implements Expression {
    public static StatefulExpression of(Expression innerExpression) {
        return ImmutableStatefulExpression.of(innerExpression);
    }

    @Value.Parameter
    public abstract Expression innerExpression();

    @Override
    public <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
