//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.function;

/**
 * Represents an operation that accepts no input and returns no result, declaring a possibly-thrown exception.
 *
 * @param <E> The type of the exception that may be thrown
 */
@FunctionalInterface
public interface ThrowingRunnable<E extends Exception> {

    /**
     * See {@link Runnable#run()}.
     */
    void run() throws E;
}
