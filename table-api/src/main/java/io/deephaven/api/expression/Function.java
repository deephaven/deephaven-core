//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.expression;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterNot;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * Represents a function call.
 */
@Immutable
@BuildableStyle
public abstract class Function implements Expression, Filter {

    public static Builder builder() {
        return ImmutableFunction.builder();
    }

    public static Function of(String name, Expression... arguments) {
        return builder().name(name).addArguments(arguments).build();
    }

    public static Function of(String name, List<? extends Expression> arguments) {
        return builder().name(name).addAllArguments(arguments).build();
    }

    /**
     * The function name.
     *
     * @return the name
     */
    public abstract String name();

    /**
     * The function arguments.
     *
     * @return the arguments
     */
    public abstract List<Expression> arguments();

    @Override
    public final FilterNot<Function> invert() {
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

    public interface Builder {
        Builder name(String name);

        Builder addArguments(Expression element);

        Builder addArguments(Expression... elements);

        Builder addAllArguments(Iterable<? extends Expression> elements);

        Function build();
    }
}
