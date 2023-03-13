package io.deephaven.api.expression;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class Minus extends BinaryExpressionBase {
    public static Minus of(Expression lhs, Expression rhs) {
        return ImmutableMinus.of(lhs, rhs);
    }

    @Parameter
    public abstract Expression lhs();

    @Parameter
    public abstract Expression rhs();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
