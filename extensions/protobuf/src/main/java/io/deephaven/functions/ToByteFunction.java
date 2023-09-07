/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A {@code byte} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface ToByteFunction<T> extends ToPrimitiveFunction<T> {

    /**
     * Assumes the object value is directly castable to a byte. Equivalent to {@code x -> (byte)x}.
     *
     * @return the byte function
     * @param <T> the value type
     */
    static <T> ToByteFunction<T> cast() {
        return ByteFunctions.cast();
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsByte(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the byte function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> ToByteFunction<T> map(Function<T, R> f, ToByteFunction<R> g) {
        return ByteFunctions.map(f, g);
    }

    /**
     * Applies this function to the given argument.
     *
     * @param value the function argument
     * @return the function result
     */
    byte applyAsByte(T value);

    @Override
    default ByteType returnType() {
        return Type.byteType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
