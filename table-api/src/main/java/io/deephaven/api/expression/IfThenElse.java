/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.expression;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterNot;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class IfThenElse implements Expression, Filter {

    public static IfThenElse of(Filter condition, Expression ifTrue, Expression ifFalse) {
        return ImmutableIfThenElse.of(condition, ifTrue, ifFalse);
    }

    @Parameter
    public abstract Filter condition();

    @Parameter
    public abstract Expression ifTrue();

    @Parameter
    public abstract Expression ifFalse();

    @Override
    public final FilterNot<IfThenElse> invert() {
        return Filter.not(this);
    }

    @Override
    public final <T> T walk(Expression.Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final <T> T walk(Filter.Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
