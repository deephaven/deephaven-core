//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A {@link JobScheduler} over a plain {@link Executor}. A job the executor refuses with a
 * {@link RejectedExecutionException}, as a bounded pool with no queue does when all of its threads are busy, runs on
 * the submitting thread instead, so no job is ever dropped and no job ever waits in a queue behind another.
 *
 * <p>
 * It is meant for a pool shaped like {@link #newHelperPool}, which may be small and shared by many callers, and on
 * which a job may itself invoke a nested iteration; the pool's size bounds the threads a caller adds to its own, never
 * the work it can finish. {@link #invokeParallel} works with any executor, because its caller never waits on a helper
 * that has not started. A task that hands its completion to a nested callback-form iteration through {@code resume}
 * does wait on that iteration's jobs, though, so with an executor that queues, such a task must not run on a thread the
 * queued jobs need: with this pool shape a job either starts at once or runs on its submitter, so that cannot happen.
 * </p>
 *
 * <p>
 * Like the other schedulers, an instance accumulates the performance of the jobs that ran off the submitting thread, so
 * make one per operation, over an executor that outlives it.
 * </p>
 */
public class ExecutorJobScheduler implements JobScheduler {

    /** How long a thread of a pool made by {@link #newHelperPool} waits for work before it exits. */
    private static final long HELPER_POOL_KEEP_ALIVE_SECONDS = 60;

    private final BasePerformanceEntry accumulatedBaseEntry = new BasePerformanceEntry();
    private final Executor executor;
    private final int threadCount;

    /**
     * @param executor runs the jobs; it may refuse one with a {@link RejectedExecutionException}, which then runs on
     *        the submitting thread
     * @param threadCount the most threads, the submitting thread included, that one iteration on this scheduler should
     *        use; an iteration asks the executor for one fewer than this when the caller takes part
     */
    public ExecutorJobScheduler(@NotNull final Executor executor, final int threadCount) {
        this.executor = Require.neqNull(executor, "executor");
        this.threadCount = Require.geq(threadCount, "threadCount", 1);
    }

    /**
     * Make the pool this scheduler is designed for: up to {@code maxThreads} threads, made when needed and retired
     * after a minute without work, and no queue, so that a job either gets a thread at once or is refused. The pool is
     * never shut down by this class; give it daemon threads.
     *
     * @param maxThreads the most threads the pool holds
     * @param threadFactory makes the pool's threads
     * @return the pool
     */
    public static ThreadPoolExecutor newHelperPool(final int maxThreads, @NotNull final ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(0, Require.geq(maxThreads, "maxThreads", 1),
                HELPER_POOL_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
    }

    @Override
    public void submit(
            final ExecutionContext executionContext,
            final Runnable runnable,
            final LogOutputAppendable description,
            final Consumer<Exception> onError) {
        try {
            executor.execute(() -> {
                final BasePerformanceEntry baseEntry = new BasePerformanceEntry();
                baseEntry.onBaseEntryStart();
                try {
                    JobScheduler.runJob(executionContext, runnable, description, onError);
                } finally {
                    baseEntry.onBaseEntryEnd();
                    accumulatedBaseEntry.accumulate(baseEntry);
                }
            });
        } catch (final RejectedExecutionException e) {
            // Every thread is busy: the job is the submitting thread's own work, and is accounted as such
            JobScheduler.runJob(executionContext, runnable, description, onError);
        }
    }

    @Override
    public BasePerformanceEntry getAccumulatedPerformance() {
        return accumulatedBaseEntry;
    }

    @Override
    public int threadCount() {
        return threadCount;
    }
}
