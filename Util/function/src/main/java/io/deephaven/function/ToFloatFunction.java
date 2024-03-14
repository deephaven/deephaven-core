//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A {@code float} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface ToFloatFunction<T> extends ToPrimitiveFunction<T> {
    /**
     * Assumes the object value is directly castable to a float. Equivalent to {@code x -> (float)x}.
     *
     * @return the float function
     * @param <T> the value type
     */
    static <T> ToFloatFunction<T> cast() {
        return FloatFunctions.cast();
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsFloat(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the float function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> ToFloatFunction<T> map(
            Function<? super T, ? extends R> f,
            ToFloatFunction<? super R> g) {
        return FloatFunctions.map(f, g);
    }

    /**
     * Applies this function to the given argument.
     *
     * @param value the function argument
     * @return the function result
     */
    float applyAsFloat(T value);

    @Override
    default FloatType returnType() {
        return Type.floatType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
