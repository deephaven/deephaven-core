package io.deephaven.grpc_api.util;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.utils.TimeProvider;
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
    void runAtTime(@NotNull DBDateTime absoluteTime, @NotNull Runnable command);

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
     * Schedule this task to run immediately, under the exclusive LTM lock.
     * 
     * @param command the task to run
     */
    void runSerially(@NotNull Runnable command);

    class DelegatingImpl implements Scheduler {

        private final ExecutorService serialDelegate;
        private final ScheduledExecutorService concurrentDelegate;

        public DelegatingImpl(final ExecutorService serialExecutor,
            final ScheduledExecutorService concurrentExecutor) {
            this.serialDelegate = serialExecutor;
            this.concurrentDelegate = concurrentExecutor;
        }

        @Override
        public DBDateTime currentTime() {
            return DBTimeUtils.currentTime();
        }

        @Override
        public void runAtTime(@NotNull final DBDateTime absoluteTime,
            final @NotNull Runnable command) {
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
