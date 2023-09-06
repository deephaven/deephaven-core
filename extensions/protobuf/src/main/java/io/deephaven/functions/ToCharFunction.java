/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A {@code char} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface ToCharFunction<T> extends ToPrimitiveFunction<T> {
    /**
     * Assumes the object value is directly castable to a char. Equivalent to {@code x -> (char)x}.
     *
     * @return the char function
     * @param <T> the value type
     */
    static <T> ToCharFunction<T> cast() {
        return CharFunctions.cast();
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsChar(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the char function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> ToCharFunction<T> map(Function<T, R> f, ToCharFunction<R> g) {
        return CharFunctions.map(f, g);
    }

    /**
     * Applies this function to the given argument.
     *
     * @param value the function argument
     * @return the function result
     */
    char applyAsChar(T value);

    @Override
    default CharType returnType() {
        return Type.charType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
