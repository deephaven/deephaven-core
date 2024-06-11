//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.function;

/**
 * Functional interface to apply an operation to an object and produce a {@code char}.
 *
 * @param <T> the object type that this function applies to
 */
@FunctionalInterface
public interface ToCharFunction<T> {
    /**
     * Applies this function to the given argument of type {@link T}.
     * 
     * @param value the argument to the function
     * @return the char result
     */
    char applyAsChar(T value);
}
