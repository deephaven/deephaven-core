package io.deephaven.api.expression;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class ExpressionFunction implements Expression {

    public static Builder builder() {
        return ImmutableExpressionFunction.builder();
    }

    public abstract String name();

    public abstract List<Expression> arguments();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {
        Builder name(String name);

        Builder addArguments(Expression element);

        Builder addArguments(Expression... elements);

        Builder addAllArguments(Iterable<? extends Expression> elements);

        ExpressionFunction build();
    }
}
