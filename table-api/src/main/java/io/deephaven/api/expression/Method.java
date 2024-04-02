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
 * Represents a method call.
 */
@Immutable
@BuildableStyle
public abstract class Method implements Expression, Filter {

    public static Builder builder() {
        return ImmutableMethod.builder();
    }

    public static Method of(Expression object, String name, Expression... arguments) {
        return builder().object(object).name(name).addArguments(arguments).build();
    }

    public static Method of(Expression object, String name, List<? extends Expression> arguments) {
        return builder().object(object).name(name).addAllArguments(arguments).build();
    }

    /**
     * The method object.
     *
     * @return the method object
     */
    public abstract Expression object();

    /**
     * The method name.
     *
     * @return the method name
     */
    public abstract String name();

    /**
     * The method arguments.
     *
     * @return the method arguments
     */
    public abstract List<Expression> arguments();

    @Override
    public final FilterNot<Method> invert() {
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
        Builder object(Expression object);

        Builder name(String name);

        Builder addArguments(Expression element);

        Builder addArguments(Expression... elements);

        Builder addAllArguments(Iterable<? extends Expression> elements);

        Method build();
    }
}
