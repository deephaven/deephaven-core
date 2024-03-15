//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.util;

import io.deephaven.base.clock.Clock;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The Scheduler is used to schedule tasks that should execute at a future time.
 */
public interface Scheduler extends Clock {

    /**
     * Schedule this task to run at the specified time.
     *
     * @param epochMillis when to run this task
     * @param command the task to run
     */
    void runAtTime(long epochMillis, @NotNull Runnable command);

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
        private final Clock clock;

        public DelegatingImpl(ExecutorService serialExecutor, ScheduledExecutorService concurrentExecutor,
                Clock clock) {
            this.serialDelegate = Objects.requireNonNull(serialExecutor);
            this.concurrentDelegate = Objects.requireNonNull(concurrentExecutor);
            this.clock = Objects.requireNonNull(clock);
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
        public long currentTimeMillis() {
            return clock.currentTimeMillis();
        }

        @Override
        public long currentTimeMicros() {
            return clock.currentTimeMicros();
        }

        @Override
        public long currentTimeNanos() {
            return clock.currentTimeNanos();
        }

        @Override
        public Instant instantNanos() {
            return clock.instantNanos();
        }

        @Override
        public Instant instantMillis() {
            return clock.instantMillis();
        }

        @Override
        public void runAtTime(long epochMillis, @NotNull Runnable command) {
            runAfterDelay(epochMillis - clock.currentTimeMillis(), command);
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
