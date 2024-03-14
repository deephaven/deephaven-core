//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.function;

/**
 * Represents an operation that accepts two input arguments and returns no result, declaring a possibly-thrown
 * exception.
 *
 * @param <T> The type of the first input to the consumer
 * @param <U> The type of the second input to the consumer
 * @param <E> The type of the exception that may be thrown
 */
@FunctionalInterface
public interface ThrowingBiConsumer<T, U, E extends Exception> {

    /**
     * See {@link java.util.function.BiConsumer#accept(Object, Object)}.
     */
    void accept(T t, U u) throws E;
}
