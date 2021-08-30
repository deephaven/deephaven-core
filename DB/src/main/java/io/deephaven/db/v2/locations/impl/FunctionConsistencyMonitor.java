package io.deephaven.db.v2.locations.impl;

import io.deephaven.util.SafeCloseable;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class FunctionConsistencyMonitor {
    /**
     * If a user function returns null, we store this value; which enables us to return null for
     * real and distinguish from a null because the function has not been called yet.
     */
    private static final Object NULL_SENTINEL_OBJECT = new Object();

    /**
     * How many functions have been registered with this instance?
     */
    private final AtomicInteger functionCount = new AtomicInteger(0);

    /**
     * The currentValues, initialized by the {@link #startConsistentBlock()} when performing a
     * series of computations.
     */
    private final ThreadLocal<Object[]> currentValues = new ThreadLocal<>();

    /**
     * A pre-allocated array of values, that we swap into currentValues as appropriate.
     */
    private final ThreadLocal<Object[]> preparedValues = new ThreadLocal<>();

    /**
     * Get an integer token identifying your function.
     *
     * @return a token for use with getValue and computeValue
     */
    private int registerFunction() {
        return functionCount.getAndIncrement();
    }

    /**
     * Called before a user initiates a series of consistent functions.
     *
     * The primary use case is the CompositeTableDataService needs to determine which location
     * providers are responsive. Each provider must have a consistent value for formulas, such as
     * the currentDate.
     *
     * @return true if a consistent block was started (and thus must be closed); false otherwise
     */
    private boolean startConsistentBlock() {
        final int currentCount = functionCount.get();
        if (currentCount == 0) {
            return false;
        }
        if (currentValues.get() == null) {
            Object[] preparedValue = preparedValues.get();
            if (preparedValue == null || preparedValue.length < currentCount) {
                preparedValue = new Object[currentCount];
            }
            currentValues.set(preparedValue);
            return true;
        }
        return false;
    }

    /**
     * Called after the sequence of consistent functions to discard the results.
     */
    private void endConsistentBlock() {
        if (functionCount.get() == 0) {
            return;
        }
        final Object[] values = currentValues.get();
        if (values == null) {
            return;
        }
        currentValues.set(null);
        Arrays.fill(values, null);
        preparedValues.set(values);
    }

    /**
     * Compute the value for the function at location. If are outside a consistent block, just
     * return the function.
     *
     * The first time we compute a function within a consistent block, call function; otherwise
     * return the previously computed value.
     *
     * @param location the location returned from {@link #registerFunction()}
     * @param function the function to compute
     * @param <T> the return type of the function
     *
     * @return the first return value of function within a consistent block
     */
    private <T> T computeValue(int location, Supplier<T> function) {
        Object[] values = currentValues.get();
        if (values == null) {
            // we are not in a consistent block
            return function.get();
        }

        if (values.length <= location) {
            final int currentCount = functionCount.get();
            if (location >= currentCount) {
                throw new IllegalStateException(
                    "Location was not registered with this monitor " + location);
            }
            // we registered the function after creating the array for consistent invocation, update
            // the size of our slots array
            values = Arrays.copyOf(values, currentCount);
            currentValues.set(values);
        }

        final Object savedResult = values[location];
        if (savedResult == NULL_SENTINEL_OBJECT) {
            return null;
        }
        if (savedResult != null) {
            // noinspection unchecked
            return (T) savedResult;
        }
        // we haven't previously computed the function, so we must compute it now
        final T computedValue = function.get();
        values[location] = computedValue == null ? NULL_SENTINEL_OBJECT : computedValue;
        return computedValue;
    }

    /**
     * Starts a consistent block around the inputProviders.
     */
    public SafeCloseable start() {
        return new CloseBlock();
    }

    private class CloseBlock implements SafeCloseable {
        private final boolean doClose;

        CloseBlock() {
            doClose = startConsistentBlock();
        }

        @Override
        public void close() {
            if (doClose) {
                endConsistentBlock();
            }
        }
    }

    /**
     * A supplier that uses a FunctionConsistencyMonitor to ensure that multiple invocations to the
     * same function always return the same value, even if underlying conditions (like the date)
     * change.
     *
     * @param <T> the return type of this supplier
     */
    public static class ConsistentSupplier<T> implements Supplier<T> {
        private final FunctionConsistencyMonitor monitor;
        private final Supplier<T> underlyingSupplier;
        private final int id;

        public ConsistentSupplier(FunctionConsistencyMonitor monitor,
            Supplier<T> underlyingSupplier) {
            this.monitor = monitor;
            this.underlyingSupplier = underlyingSupplier;
            this.id = monitor.registerFunction();
        }

        @Override
        public T get() {
            return monitor.computeValue(id, underlyingSupplier);
        }
    }
}
