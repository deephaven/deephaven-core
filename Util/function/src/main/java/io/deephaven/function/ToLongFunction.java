//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A {@code long} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface ToLongFunction<T> extends ToPrimitiveFunction<T>, java.util.function.ToLongFunction<T> {
    /**
     * Assumes the object value is directly castable to a long. Equivalent to {@code x -> (long)x}.
     *
     * @return the long function
     * @param <T> the value type
     */
    static <T> ToLongFunction<T> cast() {
        return LongFunctions.cast();
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsLong(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the long function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> ToLongFunction<T> map(
            Function<? super T, ? extends R> f,
            java.util.function.ToLongFunction<? super R> g) {
        return LongFunctions.map(f, g);
    }

    @Override
    long applyAsLong(T value);

    @Override
    default LongType returnType() {
        return Type.longType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
