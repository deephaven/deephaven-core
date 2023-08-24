/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * An {@code int} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface IntFunction<T> extends PrimitiveFunction<T>, ToIntFunction<T> {

    /**
     * Assumes the object value is directly castable to an int. Equivalent to {@code x -> (int)x}.
     *
     * @return the int function
     * @param <T> the value type
     */
    static <T> IntFunction<T> primitive() {
        return IntFunctions.primitive();
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsInt(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the int function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> IntFunction<T> map(Function<T, R> f, IntFunction<R> g) {
        return IntFunctions.map(f, g);
    }

    @Override
    int applyAsInt(T value);

    @Override
    default IntType returnType() {
        return Type.intType();
    }

    @Override
    default IntFunction<T> mapInput(Function<T, T> f) {
        return map(f, this);
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
