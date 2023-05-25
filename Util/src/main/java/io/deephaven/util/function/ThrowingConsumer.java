package io.deephaven.util.function;

/**
 * Represents an operation that accepts a single input argument and returns no result, declaring a possibly-thrown
 * exception.
 *
 * @param <T> The type of the input to the consumer
 * @param <E> The type of the exception that may be thrown
 */
@FunctionalInterface
public interface ThrowingConsumer<T, E extends Exception> {

    /**
     * See {@link java.util.function.Consumer#accept(Object)}.
     */
    void accept(T t) throws E;
}
