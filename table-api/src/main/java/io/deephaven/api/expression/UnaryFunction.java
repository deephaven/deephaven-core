package io.deephaven.api.expression;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class UnaryFunction extends UnaryExpressionBase {
    public static UnaryFunction of(String name, Expression parent) {
        return ImmutableUnaryFunction.of(name, parent);
    }

    @Parameter
    public abstract String name();

    @Parameter
    public abstract Expression parent();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
