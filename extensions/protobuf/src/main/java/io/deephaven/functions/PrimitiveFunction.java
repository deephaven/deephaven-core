/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.PrimitiveType;

/**
 * A function interface that allows for differentiation of primitive return types.
 *
 * @param <T> the input type
 * @see BooleanFunction
 * @see CharFunction
 * @see ByteFunction
 * @see ShortFunction
 * @see IntFunction
 * @see LongFunction
 * @see FloatFunction
 * @see DoubleFunction
 */
public interface PrimitiveFunction<T> extends TypedFunction<T> {

    @Override
    PrimitiveType<?> returnType();

    @Override
    default <R> R walk(TypedFunction.Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    <R> R walk(Visitor<T, R> visitor);

    interface Visitor<T, R> {
        R visit(BooleanFunction<T> f);

        R visit(CharFunction<T> f);

        R visit(ByteFunction<T> f);

        R visit(ShortFunction<T> f);

        R visit(IntFunction<T> f);

        R visit(LongFunction<T> f);

        R visit(FloatFunction<T> f);

        R visit(DoubleFunction<T> f);
    }
}
