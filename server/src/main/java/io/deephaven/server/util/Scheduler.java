/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.util;

import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.TimeProvider;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The Scheduler is used to schedule tasks that should execute at a future time.
 */
public interface Scheduler extends TimeProvider {

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
        private final TimeProvider timeProvider;

        public DelegatingImpl(ExecutorService serialExecutor, ScheduledExecutorService concurrentExecutor,
                TimeProvider timeProvider) {
            this.serialDelegate = Objects.requireNonNull(serialExecutor);
            this.concurrentDelegate = Objects.requireNonNull(concurrentExecutor);
            this.timeProvider = Objects.requireNonNull(timeProvider);
        }

        @Override
        public DateTime currentTime() {
            return timeProvider.currentTime();
        }

        @Override
        public long currentTimeMillis() {
            return timeProvider.currentTimeMillis();
        }

        @Override
        public long currentTimeMicros() {
            return timeProvider.currentTimeMicros();
        }

        @Override
        public long currentTimeNanos() {
            return timeProvider.currentTimeNanos();
        }

        @Override
        public Instant currentTimeInstant() {
            return timeProvider.currentTimeInstant();
        }

        @Override
        public long nanoTime() {
            return timeProvider.nanoTime();
        }

        @Override
        public void runAtTime(long epochMillis, @NotNull Runnable command) {
            runAfterDelay(epochMillis - timeProvider.currentTimeMillis(), command);
        }

        @Override
        public void runImmediately(final @NotNull Runnable command) {
            runAfterDelay(0, command);
        }

        @Override
        public void runAfterDelay(final long delayMs, final @NotNull Runnable command) {
            concurrentDelegate.schedule(command, delayMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public void runSerially(final @NotNull Runnable command) {
            serialDelegate.submit(command);
        }
    }
}
