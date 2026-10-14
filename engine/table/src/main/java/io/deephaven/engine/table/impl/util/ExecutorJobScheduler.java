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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A {@link JobScheduler} over a plain {@link Executor}. A job the executor refuses with a
 * {@link RejectedExecutionException}, as a bounded pool with no queue does when all of its threads are busy, runs on
 * the submitting thread instead, so no job is ever dropped; over a pool with no queue, such as {@link #newHelperPool}
 * makes, no job ever waits in a queue behind another either.
 *
 * <p>
 * It is meant for a pool shaped like {@link #newHelperPool}, which may be small and shared by many callers, and on
 * which a job may itself invoke a nested iteration; the pool's size bounds the threads a caller adds to its own, never
 * the work it can finish.
 * </p>
 *
 * <p>
 * A caller of {@link #invokeParallel} idles only once no task is left for it to claim: it then waits for the tasks that
 * helpers already started. It never waits on a helper that has not started, since it runs any such helper itself, so
 * this holds for any executor. A task that itself calls {@code invokeParallel} is such a caller too, and works through
 * its nested iteration the same way; when the pool has no free thread, its nested helpers run on its own thread and it
 * does not wait at all.
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
    private final AtomicInteger outstandingJobs = new AtomicInteger(0);
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
        outstandingJobs.incrementAndGet();
        final Thread submitter = Thread.currentThread();
        final AtomicBoolean started = new AtomicBoolean();
        try {
            executor.execute(() -> {
                started.set(true);
                try {
                    if (Thread.currentThread() == submitter) {
                        // A synchronous executor ran it here: the submitting thread's own work, as a refused job is
                        JobScheduler.runJob(executionContext, runnable, description, onError);
                        return;
                    }
                    final BasePerformanceEntry baseEntry = new BasePerformanceEntry();
                    baseEntry.onBaseEntryStart();
                    try {
                        JobScheduler.runJob(executionContext, runnable, description, onError);
                    } finally {
                        baseEntry.onBaseEntryEnd();
                        accumulatedBaseEntry.accumulate(baseEntry);
                    }
                } finally {
                    decrementOutstandingJobs();
                }
            });
        } catch (final RejectedExecutionException e) {
            if (started.get()) {
                // A synchronous executor ran the job, which then threw this itself: the job has run, and released its
                // count, and must not run again.
                throw e;
            }
            // Every thread is busy: the job is the submitting thread's own work, and is accounted as such
            decrementOutstandingJobs();
            JobScheduler.runJob(executionContext, runnable, description, onError);
        } catch (final Throwable t) {
            // A job that never started must release its count here, or getAccumulatedPerformance would wait forever
            // for it, as in OperationInitializerJobScheduler.
            if (!started.get()) {
                decrementOutstandingJobs();
            }
            throw t;
        }
    }

    /**
     * Decrement the number of outstanding jobs, either because we could not submit the job or because the job
     * completed.
     */
    private void decrementOutstandingJobs() {
        if (outstandingJobs.decrementAndGet() == 0) {
            synchronized (outstandingJobs) {
                outstandingJobs.notifyAll();
            }
        }
    }

    @Override
    public BasePerformanceEntry getAccumulatedPerformance() {
        boolean interrupted = false;
        synchronized (outstandingJobs) {
            while (outstandingJobs.get() > 0) {
                try {
                    outstandingJobs.wait();
                } catch (InterruptedException e) {
                    // keep waiting, and restore the interrupt for the caller, which may be about to check it
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return accumulatedBaseEntry;
    }

    @Override
    public int threadCount() {
        return threadCount;
    }
}
