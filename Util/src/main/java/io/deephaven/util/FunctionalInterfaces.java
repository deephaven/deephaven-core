/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

import org.jetbrains.annotations.NotNull;

/**
 * Holder for functional interfaces not defined in the java.util.function package.
 */
public class FunctionalInterfaces {

    /**
     * Represents an operation that accepts a single input argument and returns no result, declaring a possibly-thrown
     * exception.
     *
     * @param <T> The type of the input to the consumer
     * @param <ExceptionType> The type of the exception that may be thrown
     */
    @FunctionalInterface
    public interface ThrowingConsumer<T, ExceptionType extends Exception> {
        void accept(T t) throws ExceptionType;
    }

    /**
     * Represents an operation that accepts no input and returns a result, declaring a possibly-thrown exception.
     *
     * @param <T> The type of the output of the supplier
     * @param <ExceptionType> The type of the exception that may be thrown
     */
    @FunctionalInterface
    public interface ThrowingSupplier<T, ExceptionType extends Exception> {
        T get() throws ExceptionType;

        /**
         * Allow adapting a {@link ThrowingSupplier} to the signature for {@link java.util.function.Supplier#get},
         * catching any thrown {@link Exception exceptions} and wrapping them in {@link RuntimeException runtime
         * exceptions}.
         *
         * @param supplier The supplier to adapt
         * @return The result of {@code supplier}
         * @param <T> The type of the result
         * @param <E> The type of the exception that might be thrown by {@code supplier}
         */
        static <T, E extends Exception> T wrapUnexpectedException(@NotNull ThrowingSupplier<T, E> supplier) {
            try {
                return supplier.get();
            } catch (Exception e) {
                throw new RuntimeException("Unexpected exception", e);
            }
        }
    }

    /**
     * Represents an operation that accepts no input and returns a boolean result, declaring a possibly-thrown
     * exception.
     *
     * @param <ExceptionType> The type of the exception that may be thrown
     */
    @FunctionalInterface
    public interface ThrowingBooleanSupplier<ExceptionType extends Exception> {
        boolean get() throws ExceptionType;
    }

    /**
     * Represents an operation that accepts two input arguments and returns no result, declaring a possibly-thrown
     * exception.
     *
     * @param <T> The type of the first input to the consumer
     * @param <U> The type of the second input to the consumer
     * @param <ExceptionType> The type of the exception that may be thrown
     */
    @FunctionalInterface
    public interface ThrowingBiConsumer<T, U, ExceptionType extends Exception> {
        void accept(T t, U u) throws ExceptionType;
    }

    /**
     * Represents an operation that accepts no input and returns no result, declaring a possibly-thrown exception.
     *
     * @param <ExceptionType> The type of the exception that may be thrown
     */
    @FunctionalInterface
    public interface ThrowingRunnable<ExceptionType extends Exception> {
        void run() throws ExceptionType;
    }
}
