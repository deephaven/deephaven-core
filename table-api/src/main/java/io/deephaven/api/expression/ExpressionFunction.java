package io.deephaven.api.expression;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * Represents a function call.
 */
@Immutable
@BuildableStyle
public abstract class ExpressionFunction implements Expression {

    public static Builder builder() {
        return ImmutableExpressionFunction.builder();
    }

    /**
     * The name.
     *
     * @return the name
     */
    public abstract String name();

    /**
     * The arguments.
     *
     * @return the arguments
     */
    public abstract List<Expression> arguments();

    @Override
    public final <T> T walk(Expression.Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder {
        Builder name(String name);

        Builder addArguments(Expression element);

        Builder addArguments(Expression... elements);

        Builder addAllArguments(Iterable<? extends Expression> elements);

        ExpressionFunction build();
    }
}
