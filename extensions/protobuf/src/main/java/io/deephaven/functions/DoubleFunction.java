/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;
import java.util.function.ToDoubleFunction;

/**
 * A {@code double} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface DoubleFunction<T> extends PrimitiveFunction<T>, ToDoubleFunction<T> {

    /**
     * Assumes the object value is directly castable to a double. Equivalent to {@code x -> (double)x}.
     *
     * @return the double function
     * @param <T> the value type
     */
    static <T> DoubleFunction<T> primitive() {
        return DoubleFunctions.primitive();
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
    static <T, R> DoubleFunction<T> map(Function<T, R> f, DoubleFunction<R> g) {
        return DoubleFunctions.map(f, g);
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
    static <T, R> DoubleFunction<T> map(ObjectFunction<T, R> f, DoubleFunction<R> g) {
        return DoubleFunctions.map(f, g);
    }

    @Override
    double applyAsDouble(T value);

    @Override
    default DoubleType returnType() {
        return Type.doubleType();
    }

    @Override
    default DoubleFunction<T> mapInput(Function<T, T> f) {
        return map(f, this);
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
