/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.PrimitiveType;

/**
 * A function interface that allows for differentiation of primitive return types.
 *
 * @param <T> the input type
 * @see ToBooleanFunction
 * @see ToCharFunction
 * @see ToByteFunction
 * @see ToShortFunction
 * @see ToIntFunction
 * @see ToLongFunction
 * @see ToFloatFunction
 * @see ToDoubleFunction
 */
public interface ToPrimitiveFunction<T> extends TypedFunction<T> {

    @Override
    PrimitiveType<?> returnType();

    @Override
    default <R> R walk(TypedFunction.Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    <R> R walk(Visitor<T, R> visitor);

    interface Visitor<T, R> {
        R visit(ToBooleanFunction<T> f);

        R visit(ToCharFunction<T> f);

        R visit(ToByteFunction<T> f);

        R visit(ToShortFunction<T> f);

        R visit(ToIntFunction<T> f);

        R visit(ToLongFunction<T> f);

        R visit(ToFloatFunction<T> f);

        R visit(ToDoubleFunction<T> f);
    }
}
