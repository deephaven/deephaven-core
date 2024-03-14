//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A {@code double} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface ToDoubleFunction<T> extends ToPrimitiveFunction<T>, java.util.function.ToDoubleFunction<T> {

    /**
     * Assumes the object value is directly castable to a double. Equivalent to {@code x -> (double)x}.
     *
     * @return the double function
     * @param <T> the value type
     */
    static <T> ToDoubleFunction<T> cast() {
        return DoubleFunctions.cast();
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsDouble(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the double function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> ToDoubleFunction<T> map(
            Function<? super T, ? extends R> f,
            java.util.function.ToDoubleFunction<? super R> g) {
        return DoubleFunctions.map(f, g);
    }

    @Override
    double applyAsDouble(T value);

    @Override
    default DoubleType returnType() {
        return Type.doubleType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
