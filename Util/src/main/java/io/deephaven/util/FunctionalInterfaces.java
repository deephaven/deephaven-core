package io.deephaven.util;

/**
 * Holder for functional interfaces not defined in the java.util.function package.
 */
public class FunctionalInterfaces {

    public static <T, E extends Exception> T unexpectedException(ThrowingSupplier<T, E> operator) {
        try {
            return operator.get();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception", e);
        }
    }

    /**
     * Represents an operation that accepts a single input argument and returns no result, throwing
     * an exception.
     *
     * @param <T> the type of the input to the operation
     * @param <ExceptionType> the type of the exception that can be thrown
     */
    @FunctionalInterface
    public interface ThrowingConsumer<T, ExceptionType extends Exception> {
        void accept(T t) throws ExceptionType;
    }

    /**
     * Represents an operation that accepts no input and returns a result, throwing an exception.
     *
     * @param <T> the type of the output of the operation
     * @param <ExceptionType> the type of the exception that can be thrown
     */
    @FunctionalInterface
    public interface ThrowingSupplier<T, ExceptionType extends Exception> {
        T get() throws ExceptionType;
    }

    /**
     * Represents an operation that accepts no input and returns a boolean result, throwing an
     * exception.
     *
     * @param <ExceptionType> the type of the exception that can be thrown
     */
    @FunctionalInterface
    public interface ThrowingBooleanSupplier<ExceptionType extends Exception> {
        boolean get() throws ExceptionType;
    }

    /**
     * Represents a function that accepts three arguments and produces a result.
     * 
     * @param <T> the type of the first argument to the function
     * @param <U> the type of the second argument to the function
     * @param <V> the type of the third argument to the function
     * @param <R> the type of the result of the function
     */
    public interface TriFunction<T, U, V, R> {
        /**
         * Applies this function to the given arguments.
         *
         * @param t the first function argument
         * @param u the second function argument
         * @param v the third argument
         * @return the function result
         */
        R apply(T t, U u, V v);
    }

    @FunctionalInterface
    public interface ThrowingBiConsumer<T, U, ExceptionType extends Exception> {
        void accept(T t, U u) throws ExceptionType;
    }

    @FunctionalInterface
    public interface ThrowingTriConsumer<T, U, V, ExceptionType extends Exception> {
        void accept(T t, U u, V v) throws ExceptionType;
    }

    @FunctionalInterface
    public interface ThrowingRunnable<ExceptionType extends Exception> {
        void run() throws ExceptionType;
    }
}
