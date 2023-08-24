/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A common function interface that allows for differentiation based on the return type.
 *
 * @param <T> the input type
 * @see PrimitiveFunction
 * @see ObjectFunction
 */
public interface TypedFunction<T> {

    /**
     * This function's return type.
     *
     * @return the type
     */
    Type<?> returnType();

    /**
     * Creates the function composition {@code this ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> theApplyFunction(f.apply(x))}.
     *
     * @param f the input function
     * @return the new function
     */
    TypedFunction<T> mapInput(Function<T, T> f);

    <R> R walk(Visitor<T, R> visitor);

    interface Visitor<T, R> {
        R visit(PrimitiveFunction<T> f);

        R visit(ObjectFunction<T, ?> f);
    }
}
