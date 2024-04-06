//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.function;

import org.jetbrains.annotations.NotNull;

/**
 * Represents an operation that accepts no input and returns a result, declaring a possibly-thrown exception.
 *
 * @param <T> The type of the output of the supplier
 * @param <E> The type of the exception that may be thrown
 */
@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {

    /**
     * See {@link java.util.function.Supplier#get()}.
     */
    T get() throws E;

    /**
     * Allow adapting a {@link ThrowingSupplier} to the signature for {@link java.util.function.Supplier#get}, catching
     * any thrown {@link Exception exceptions} and wrapping them in {@link RuntimeException runtime exceptions}.
     *
     * @param supplier The supplier to adapt
     * @param <T> The type of the result
     * @param <E> The type of the exception that might be thrown by {@code supplier}
     * @return The result of {@code supplier}
     */
    static <T, E extends Exception> T wrapUnexpectedException(@NotNull ThrowingSupplier<T, E> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception", e);
        }
    }
}
