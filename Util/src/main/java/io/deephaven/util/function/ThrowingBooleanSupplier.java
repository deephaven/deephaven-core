//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.function;

import java.util.function.BooleanSupplier;

/**
 * Represents an operation that accepts no input and returns a boolean result, declaring a possibly-thrown exception.
 *
 * @param <E> The type of the exception that may be thrown
 */
@FunctionalInterface
public interface ThrowingBooleanSupplier<E extends Exception> {

    /**
     * See {@link BooleanSupplier#getAsBoolean()}.
     */
    boolean getAsBoolean() throws E;
}
