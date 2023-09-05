/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * An {@code int} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface ToIntFunction<T> extends ToPrimitiveFunction<T>, java.util.function.ToIntFunction<T> {

    /**
     * Assumes the object value is directly castable to an int. Equivalent to {@code x -> (int)x}.
     *
     * @return the int function
     * @param <T> the value type
     */
    static <T> ToIntFunction<T> primitive() {
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
    static <T, R> ToIntFunction<T> map(Function<T, R> f, ToIntFunction<R> g) {
        return IntFunctions.map(f, g);
    }

    @Override
    int applyAsInt(T value);

    @Override
    default IntType returnType() {
        return Type.intType();
    }

    @Override
    default ToIntFunction<T> mapInput(Function<T, T> f) {
        return map(f, this);
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
