package io.deephaven.server.util;

import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.TimeProvider;
import org.jetbrains.annotations.NotNull;

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
     * @param absoluteTime when to run this task
     * @param command the task to run
     */
    void runAtTime(@NotNull DateTime absoluteTime, @NotNull Runnable command);

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

        public DelegatingImpl(final ExecutorService serialExecutor, final ScheduledExecutorService concurrentExecutor) {
            this.serialDelegate = serialExecutor;
            this.concurrentDelegate = concurrentExecutor;
        }

        @Override
        public DateTime currentTime() {
            return DateTimeUtils.currentTime();
        }

        @Override
        public void runAtTime(@NotNull final DateTime absoluteTime, final @NotNull Runnable command) {
            runAfterDelay(absoluteTime.getMillis() - currentTime().getMillis(), command);
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
