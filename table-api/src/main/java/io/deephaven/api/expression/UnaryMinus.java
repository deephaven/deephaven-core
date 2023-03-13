package io.deephaven.api.expression;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class UnaryMinus extends UnaryExpressionBase {
    public static UnaryMinus of(Expression parent) {
        return ImmutableUnaryMinus.of(parent);
    }

    @Parameter
    public abstract Expression parent();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
