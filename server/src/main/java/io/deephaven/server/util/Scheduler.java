//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.util;

import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The Scheduler is used to schedule tasks that should execute at a future time.
 *
 * <p>
 * Scheduling uses monotonic time, which is unaffected by wall-clock adjustments. Code that needs wall-clock time should
 * use a {@link io.deephaven.base.clock.Clock} instead.
 */
public interface Scheduler {

    /**
     * Monotonic time for pacing work scheduled with {@link #runAfterDelay}. A task scheduled with a delay of {@code d}
     * milliseconds does not run before {@code monotonicTimeMillis() + d}. Values are never negative and never decrease;
     * the origin is no later than the creation of this scheduler.
     *
     * @return monotonic milliseconds
     */
    long monotonicTimeMillis();

    /**
     * Schedule this task to run at the specified time.
     *
     * @param delayMs how long to delay before running this task (in milliseconds)
     * @param command the task to run
     */
    void runAfterDelay(long delayMs, @NotNull Runnable command);

    /**
     * Schedule this task to run immediately.
     *
     * @param command the task to run
     */
    void runImmediately(@NotNull Runnable command);

    /**
     * Schedule this task to run immediately, under the exclusive UGP lock.
     *
     * @param command the task to run
     */
    void runSerially(@NotNull Runnable command);

    /**
     * @return whether this scheduler is being run for tests.
     */
    default boolean inTestMode() {
        return false;
    }

    class DelegatingImpl implements Scheduler {

        private final ExecutorService serialDelegate;
        private final ScheduledExecutorService concurrentDelegate;
        private final long originNanos = System.nanoTime();

        public DelegatingImpl(ExecutorService serialExecutor, ScheduledExecutorService concurrentExecutor) {
            this.serialDelegate = Objects.requireNonNull(serialExecutor);
            this.concurrentDelegate = Objects.requireNonNull(concurrentExecutor);
        }

        @VisibleForTesting
        public void shutdown() throws InterruptedException {
            concurrentDelegate.shutdownNow();
            serialDelegate.shutdownNow();
            if (!concurrentDelegate.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("concurrentDelegate not shutdown within 5 seconds");
            }
            if (!serialDelegate.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("serialDelegate not shutdown within 5 seconds");
            }
        }

        @Override
        public long monotonicTimeMillis() {
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - originNanos);
        }

        @Override
        public void runImmediately(@NotNull final Runnable command) {
            runAfterDelay(0, command);
        }

        @Override
        public void runAfterDelay(final long delayMs, @NotNull final Runnable command) {
            concurrentDelegate.schedule(command, delayMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public void runSerially(@NotNull final Runnable command) {
            serialDelegate.submit(command);
        }
    }
}
