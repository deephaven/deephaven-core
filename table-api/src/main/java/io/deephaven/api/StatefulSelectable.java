/**
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.StatefulExpression;
import org.immutables.value.Value;

/**
 * A {@link Selectable} that wraps another {@code Selectable}, and is used to indicate that the wrapped selectable has
 * side effects that prevent parallelization. (e.g. an expression that depends on processing rows in order, or would
 * suffer from lock contention if parallelized)
 */
@Value.Immutable
@SimpleStyle
public abstract class StatefulSelectable implements Selectable {
    public static StatefulSelectable of(Selectable inner) {
        return ImmutableStatefulSelectable.of(inner);
    }

    @Value.Parameter
    public abstract Selectable innerSelectable();

    @Override
    public ColumnName newColumn() {
        return innerSelectable().newColumn();
    }

    @Override
    public Expression expression() {
        return StatefulExpression.of(innerSelectable().expression());
    }
}
