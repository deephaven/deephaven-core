//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A {@code short} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface ToShortFunction<T> extends ToPrimitiveFunction<T> {

    /**
     * Assumes the object value is directly castable to a short. Equivalent to {@code x -> (short)x}.
     *
     * @return the short function
     * @param <T> the value type
     */
    static <T> ToShortFunction<T> cast() {
        return ShortFunctions.cast();
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsShort(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the short function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> ToShortFunction<T> map(
            Function<? super T, ? extends R> f,
            ToShortFunction<? super R> g) {
        return ShortFunctions.map(f, g);
    }

    /**
     * Applies this function to the given argument.
     *
     * @param value the function argument
     * @return the function result
     */
    short applyAsShort(T value);

    @Override
    default ShortType returnType() {
        return Type.shortType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
