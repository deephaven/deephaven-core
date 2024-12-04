//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.io.log.LogEntry;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.function.ThrowingSupplier;
import io.deephaven.util.locks.AwareFunctionalLock;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.locks.Condition;
import java.util.function.Supplier;

public interface UpdateGraph extends UpdateSourceRegistrar, NotificationQueue, NotificationQueue.Dependency {

    // region general accessors

    /**
     * @return The name of this UpdateGraph
     */
    String getName();

    /**
     * @return The shared {@link AwareFunctionalLock} to use with this update graph
     */
    AwareFunctionalLock sharedLock();

    /**
     * @return The exclusive {@link AwareFunctionalLock} to use with this update graph
     */
    AwareFunctionalLock exclusiveLock();

    /**
     * @return The {@link LogicalClock} to use with this update graph
     */
    LogicalClock clock();

    /**
     * Retrieve the number of independent update propagation tasks this UpdateGraph can process concurrently.
     * <p>
     * For example, an implementation using a fixed-size thread pool of update task workers should return the size of
     * the thread pool.
     * <p>
     * This is exposed in order to allow users to determine the ideal way to partition their queries for maximum
     * parallelism without undue overhead.
     *
     * @return number of independent update propagation tasks this UpdateGraph can process concurrently
     */
    int parallelismFactor();

    /**
     * Cast {@code this} to a more specific UpdateGraph type, in order to access implementation-specific methods.
     *
     * @param <UG_TYPE> The UpdateGraph type to cast to
     * @return {@code this}
     */
    default <UG_TYPE extends UpdateGraph> UG_TYPE cast() {
        // noinspection unchecked
        return (UG_TYPE) this;
    }

    // endregion general accessors

    // region notification support tools

    /**
     * @return A LogEntry that may be prefixed with UpdateGraph information
     */
    LogEntry logDependencies();

    // endregion notification support tools

    // region thread control

    /**
     * Test if the current thread is involved in processing updates for this UpdateGraph. If so, non-terminal user
     * notifications on the current thread must not attempt to lock this UpdateGraph.
     *
     * @return Whether the current thread is involved in processing updates for this UpdateGraph
     */
    boolean currentThreadProcessesUpdates();

    /**
     * Test if engine code executing on the current thread should assume safety for serial table operations. Operations
     * annotated as concurrent are always safe.
     *
     * @return Whether code on this thread should assume serial table operation safety
     * @see #checkInitiateSerialTableOperation()
     * @see #setSerialTableOperationsSafe(boolean)
     * @see #allowSerialTableOperations(Supplier)
     * @see #allowSerialTableOperations(ThrowingSupplier)
     */
    boolean serialTableOperationsSafe();

    /**
     * User or engine code that makes its own determination about the safety of initiating serial table operations on
     * the current thread may use this method to override default behavior. The previous value should be restored
     * immediately after use, typically with the following pattern:
     * 
     * <pre>
     * boolean oldValue = assumeSerialTableOperations(true);
     * try {
     *     // ... safe table operations here
     * } finally {
     *     assumeSerialTableOperations(oldValue);
     * }
     * </pre>
     *
     * @param newValue the new value
     * @return the old value
     * @see #serialTableOperationsSafe()
     * @see #allowSerialTableOperations(Supplier)
     * @see #allowSerialTableOperations(ThrowingSupplier)
     */
    boolean setSerialTableOperationsSafe(boolean newValue);

    /**
     * User or engine code that is certain a particular table operation is safe to execute with respect to this
     * UpdateGraph may use this method to follow the prescribed pattern for declaring safety and reinstating the
     * priority safety parameters.
     *
     * @param operation The safe operation to perform
     * @param <RETURN_TYPE> The return type of the operation
     * @return The result of {@code operation}
     * @see #serialTableOperationsSafe()
     */
    default <RETURN_TYPE> RETURN_TYPE allowSerialTableOperations(@NotNull final Supplier<RETURN_TYPE> operation) {
        final boolean oldValue = setSerialTableOperationsSafe(true);
        try {
            return operation.get();
        } finally {
            setSerialTableOperationsSafe(oldValue);
        }
    }

    /**
     * User or engine code that is certain a particular table operation is safe to execute with respect to this
     * UpdateGraph may use this method to follow the prescribed pattern for declaring safety and reinstating the
     * priority safety parameters.
     *
     * @param operation The safe operation to perform
     * @param <RETURN_TYPE> The return type of the operation
     * @param <EXCEPTION_TYPE> The exception type the operation might throw
     * @return The result of {@code operation}
     * @throws EXCEPTION_TYPE if {@code operation} throws
     * @see #serialTableOperationsSafe()
     */
    default <RETURN_TYPE, EXCEPTION_TYPE extends Exception> RETURN_TYPE allowSerialTableOperations(
            @NotNull final ThrowingSupplier<RETURN_TYPE, EXCEPTION_TYPE> operation) throws EXCEPTION_TYPE {
        final boolean oldValue = setSerialTableOperationsSafe(true);
        try {
            return operation.get();
        } finally {
            setSerialTableOperationsSafe(oldValue);
        }
    }

    /**
     * If we initiate a serial (not annotated as concurrent) table operation that should update using this UpdateGraph
     * without holding the appropriate lock, then we are likely committing a grievous error, but one that will only
     * occasionally result in us getting the wrong answer or if we are lucky an assertion. This method is called from
     * various table operations that should not be established without locking their UpdateGraph.
     * <p>
     * Threads that process this UpdateGraph's updates are assumed to be safe; if dependencies are tracked correctly,
     * these threads will only initiate table operations when they can proceed.
     * <p>
     * User or engine code may bypass this check using {@link #setSerialTableOperationsSafe(boolean)} or the related
     * wrapper methods.
     * 
     * @see #serialTableOperationsSafe() ()
     * @see #setSerialTableOperationsSafe(boolean)
     * @see #allowSerialTableOperations(Supplier)
     * @see #allowSerialTableOperations(ThrowingSupplier)
     */
    default void checkInitiateSerialTableOperation() {
        if (serialTableOperationsSafe()
                || exclusiveLock().isHeldByCurrentThread()
                || sharedLock().isHeldByCurrentThread()
                || currentThreadProcessesUpdates()) {
            return;
        }
        throw new IllegalStateException(String.format(
                "May not initiate serial table operations for update graph %s: exclusiveLockHeld=%s, sharedLockHeld=%s, currentThreadProcessesUpdates=%s",
                getName(),
                exclusiveLock().isHeldByCurrentThread(),
                sharedLock().isHeldByCurrentThread(),
                currentThreadProcessesUpdates()));
    }

    /**
     * Attempt to stop this update graph, and cease processing further notifications.
     */
    void stop();

    // endregion thread control

    // region refresh control

    /**
     * @return Whether this UpdateGraph has a mechanism that supports refreshing
     */
    boolean supportsRefreshing();

    /**
     * Request that this UpdateGraph process any pending updates as soon as practicable. Updates "hurried" in this way
     * are otherwise processed as normal.
     */
    void requestRefresh();

    /**
     * Request that a {@link Condition} derived from this UpdateGraph's {@link #exclusiveLock()} be
     * {@link Condition#signalAll() signalled} in a safe manner. This may take place asynchronously.
     * 
     * @param exclusiveLockCondition The condition to signal
     */
    default void requestSignal(Condition exclusiveLockCondition) {
        if (exclusiveLock().isHeldByCurrentThread()) {
            exclusiveLockCondition.signalAll();
        } else {
            // terminal notifications always run on the UGP thread
            final Notification terminalNotification = new TerminalNotification() {
                @Override
                public void run() {
                    Assert.assertion(exclusiveLock().isHeldByCurrentThread(),
                            "exclusiveLock().isHeldByCurrentThread()");
                    exclusiveLockCondition.signalAll();
                }

                @Override
                public boolean mustExecuteWithUpdateGraphLock() {
                    return true;
                }

                @Override
                public LogOutput append(LogOutput output) {
                    return output.append("SignalNotification(")
                            .append(System.identityHashCode(exclusiveLockCondition)).append(")");
                }
            };
            addNotification(terminalNotification);
        }
    }

    // endregion refresh control

    /**
     * Run {@code task} immediately if this UpdateGraph is currently idle, else schedule {@code task} to run at a later
     * time when it has become idle.
     *
     * @param task The task to run when idle
     */
    @FinalDefault
    default void runWhenIdle(@NotNull final Runnable task) {
        if (clock().currentState() == LogicalClock.State.Idle) {
            task.run();
        } else {
            addNotification(new TerminalNotification() {
                @Override
                public void run() {
                    task.run();
                }
            });
        }
    }
}
