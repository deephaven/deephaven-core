package io.deephaven.util.locks;

import io.deephaven.util.FunctionalInterfaces.ThrowingBooleanSupplier;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.locks.Lock;

import static io.deephaven.util.FunctionalInterfaces.ThrowingRunnable;
import static io.deephaven.util.FunctionalInterfaces.ThrowingSupplier;

/**
 * Extension to the {@link Lock} interface to enable locking for the duration of a lambda or other
 * {@link FunctionalInterface} invocation.
 */
public interface FunctionalLock extends Lock {

    /**
     * Acquire the lock, invoke {@link ThrowingRunnable#run()} while holding the lock, and release
     * the lock before returning.
     *
     * @param runnable The {@link ThrowingRunnable} to run
     * @throws EXCEPTION_TYPE If {@code runnable} throws its declared exception
     */
    default <EXCEPTION_TYPE extends Exception> void doLocked(
        @NotNull final ThrowingRunnable<EXCEPTION_TYPE> runnable) throws EXCEPTION_TYPE {
        lock();
        try {
            runnable.run();
        } finally {
            unlock();
        }
    }

    /**
     * Acquire the lock interruptibly, invoke {@link ThrowingRunnable#run()} while holding the lock,
     * and release the lock before returning.
     *
     * @param runnable The {@link ThrowingRunnable#run()} to run
     * @throws InterruptedException If the current thread was interrupted while waiting to acquire
     *         the lock
     * @throws EXCEPTION_TYPE If {@code runnable} throws its declared exception
     */
    default <EXCEPTION_TYPE extends Exception> void doLockedInterruptibly(
        @NotNull final ThrowingRunnable<EXCEPTION_TYPE> runnable)
        throws InterruptedException, EXCEPTION_TYPE {
        lockInterruptibly();
        try {
            runnable.run();
        } finally {
            unlock();
        }
    }

    /**
     * Acquire the lock, invoke {@link ThrowingSupplier#get()} while holding the lock, and release
     * the lock before returning the result.
     *
     * @param supplier The {@link ThrowingSupplier} to get
     * @return The result of invoking {@code supplier}
     * @throws EXCEPTION_TYPE If {@code supplier} throws its declared exception
     */
    default <RESULT_TYPE, EXCEPTION_TYPE extends Exception> RESULT_TYPE computeLocked(
        @NotNull final ThrowingSupplier<RESULT_TYPE, EXCEPTION_TYPE> supplier)
        throws EXCEPTION_TYPE {
        lock();
        try {
            return supplier.get();
        } finally {
            unlock();
        }
    }

    /**
     * Acquire the lock, invoke {@link ThrowingBooleanSupplier#get()} while holding the lock, and
     * release the lock before returning the result.
     *
     * @param supplier The {@link ThrowingBooleanSupplier} to get
     * @return The result of invoking {@code supplier}
     * @throws EXCEPTION_TYPE If {@code supplier} throws its declared exception
     */
    default <EXCEPTION_TYPE extends Exception> boolean testLocked(
        @NotNull final ThrowingBooleanSupplier<EXCEPTION_TYPE> supplier) throws EXCEPTION_TYPE {
        lock();
        try {
            return supplier.get();
        } finally {
            unlock();
        }
    }

    /**
     * Acquire the lock interruptibly, invoke {@link ThrowingSupplier#get()} while holding the lock,
     * and release the lock before returning the result.
     *
     * @param supplier The {@link ThrowingSupplier} to get
     * @return The result of invoking {@code supplier}
     * @throws InterruptedException If the current thread was interrupted while waiting to acquire
     *         the lock
     * @throws EXCEPTION_TYPE If {@code supplier} throws its declared exception
     */
    default <RESULT_TYPE, EXCEPTION_TYPE extends Exception> RESULT_TYPE computeLockedInterruptibly(
        @NotNull final ThrowingSupplier<RESULT_TYPE, EXCEPTION_TYPE> supplier)
        throws InterruptedException, EXCEPTION_TYPE {
        lockInterruptibly();
        try {
            return supplier.get();
        } finally {
            unlock();
        }
    }
}
