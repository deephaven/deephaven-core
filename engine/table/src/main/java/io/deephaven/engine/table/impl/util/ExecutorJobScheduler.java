//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.updategraph.UpdateGraphWaitGuard;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

/**
 * A {@link JobScheduler} over a plain {@link Executor}. A job the executor refuses, with a
 * {@link RejectedExecutionException} or anything else it throws, is withdrawn, and {@link #submit} rethrows the
 * refusal.
 *
 * <p>
 * It is meant for a pool shaped like {@link #newHelperPool}, which may be small and shared by many callers, and on
 * which a job may itself invoke a nested iteration; the pool's size bounds the threads a caller adds to its own, never
 * the work it can finish. That pool queues a job while all of its threads are busy, so a thread that finishes another
 * caller's job then helps with this one, and a callback-form iteration returns once its jobs are submitted. A job on
 * the pool must therefore not block waiting for a callback-form iteration or another submitted job, which may be queued
 * behind it, except through {@link #invokeParallel} or {@link #awaitCompletion}, which run the work themselves when no
 * thread is free.
 * </p>
 *
 * <p>
 * A caller of {@link #invokeParallel} idles only once no task is left for it to claim: it then waits for the tasks that
 * helpers already started. It never waits on a helper that has not started, since it runs any such helper itself,
 * withdrawing its queued job, so this holds for any executor, and a queue never makes it wait behind other callers'
 * jobs. A task that itself calls {@code invokeParallel} is such a caller too, and works through its nested iteration
 * the same way; when the pool has no free thread, it runs its nested helpers itself and does not wait at all.
 * </p>
 *
 * <p>
 * A thread waiting in {@link #awaitCompletion} likewise runs any of this scheduler's jobs that no thread has started,
 * and blocks only while every job not yet finished is running on another thread. A job that such a thread runs is part
 * of its own work, and its performance is not accumulated here.
 * </p>
 *
 * <p>
 * A job may not wait for other threads when the thread that submitted it may not: one that holds an update graph's lock
 * or processes its updates, or that {@link UpdateGraphWaitGuard#forbidWaits() forbids waits}. The thread may be waiting
 * for the job, and a job that waited in turn for the update graph could deadlock with it.
 * </p>
 *
 * <p>
 * An instance accumulates the performance of the jobs whose time no waiting thread already measures as its own, so make
 * one per operation, over an executor that outlives it. It leaves out a job the executor runs inside {@link #submit},
 * one a thread waiting in {@link #awaitCompletion} runs, and one that runs nested inside another of its jobs on the
 * same thread, as a fork/join pool's thread does when it helps the pool quiesce.
 * </p>
 */
public class ExecutorJobScheduler implements JobScheduler {

    /** How long a thread of a pool made by {@link #newHelperPool} waits for work before it exits. */
    private static final long HELPER_POOL_KEEP_ALIVE_SECONDS = 60;

    private final BasePerformanceEntry accumulatedBaseEntry = new BasePerformanceEntry();
    /**
     * The jobs submitted that have not finished or been withdrawn, whichever thread runs them. Its monitor guards the
     * waits for jobs, and is notified when a job is queued for waiting threads, when the count reaches zero, and when a
     * future being waited for completes.
     */
    private final AtomicInteger outstandingJobs = new AtomicInteger(0);
    /**
     * The jobs the executor has accepted, in order, for a waiting thread to claim. A job an executor thread claims or
     * the submitter withdraws is removed; any other claimed job is discarded when a waiting thread polls it.
     */
    private final Queue<Job> pendingJobs = new ConcurrentLinkedQueue<>();
    /**
     * The jobs of this scheduler that the current thread is running, or null when it runs none, so that no entry
     * outlives the jobs on a thread. None of them may wait in {@link #awaitCompletion}, which waits for every job, the
     * waiting job included.
     */
    private final ThreadLocal<MutableInt> jobsRunningOnThisThread = new ThreadLocal<>();
    private final Executor executor;
    private final IntSupplier threadCount;

    /**
     * @param executor runs the jobs; it may refuse one with a {@link RejectedExecutionException}, which {@link #submit}
     *        then rethrows
     * @param threadCount the most threads, the submitting thread included, that one iteration on this scheduler should
     *        use; an iteration asks the executor for one fewer than this when the caller takes part
     */
    public ExecutorJobScheduler(@NotNull final Executor executor, final int threadCount) {
        this(executor, constant(Require.geq(threadCount, "threadCount", 1)));
    }

    /**
     * @param executor runs the jobs; it may refuse one with a {@link RejectedExecutionException}, which {@link #submit}
     *        then rethrows
     * @param threadCount supplies {@link #threadCount()} each time it is called, so that a scheduler over an executor
     *        whose parallelism cannot be read, as a poisoned operation initializer's cannot, fails only once used
     */
    protected ExecutorJobScheduler(@NotNull final Executor executor, @NotNull final IntSupplier threadCount) {
        this.executor = Require.neqNull(executor, "executor");
        this.threadCount = Require.neqNull(threadCount, "threadCount");
    }

    private static IntSupplier constant(final int value) {
        return () -> value;
    }

    /**
     * Make the pool this scheduler is designed for: up to {@code maxThreads} threads, made when needed and retired
     * after a minute without work, and an unbounded queue, so that a job submitted while every thread is busy waits for
     * the first to finish rather than being refused. The pool is never shut down by this class; give it daemon threads.
     *
     * @param maxThreads the most threads the pool holds
     * @param threadFactory makes the pool's threads
     * @return the pool
     */
    public static ThreadPoolExecutor newHelperPool(final int maxThreads, @NotNull final ThreadFactory threadFactory) {
        final int threads = Require.geq(maxThreads, "maxThreads", 1);
        final ThreadPoolExecutor pool = new ThreadPoolExecutor(threads, threads,
                HELPER_POOL_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), threadFactory);
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    @Override
    public Submission submit(
            final ExecutionContext executionContext,
            final Runnable runnable,
            final LogOutputAppendable description,
            final Consumer<Exception> onError) {
        // The submitting thread waits for its jobs. When it must not wait for other threads, a job that waited for an
        // update graph, or for another thread that may itself be waiting for one, could deadlock with it.
        final boolean waitsForbidden = UpdateGraphWaitGuard.currentThreadMustNotWaitForOtherThreads(
                ExecutionContext.getContext().getUpdateGraph())
                || (executionContext != null && UpdateGraphWaitGuard.currentThreadMustNotWaitForOtherThreads(
                        executionContext.getUpdateGraph()));
        outstandingJobs.incrementAndGet();
        final Job job = new Job(executionContext, runnable, description, onError, waitsForbidden);
        try {
            job.inExecute = true;
            try {
                executor.execute(job);
            } finally {
                job.inExecute = false;
            }
        } catch (final Throwable t) {
            // A job that never started is withdrawn here, releasing its count, or getAccumulatedPerformance would wait
            // forever for it; an Error counts too, OutOfMemoryError when the pool cannot make a thread in practice. A
            // job that a synchronous executor ran before throwing has released its count itself, and is not withdrawn.
            job.withdraw();
            throw t;
        }
        // Waiting threads see only accepted jobs. One a thread has already claimed is skipped here, or discarded when a
        // waiting thread polls it.
        if (!job.isClaimed()) {
            pendingJobs.add(job);
            signalWaiters();
        }
        return job;
    }

    @Override
    public <T> T awaitCompletion(@NotNull final CompletableFuture<T> future)
            throws InterruptedException, ExecutionException {
        Assert.eqNull(jobsRunningOnThisThread.get(), "jobs of this scheduler running on the waiting thread");
        // reading a thread local with no value installs its null initial value, which the waiting thread does not keep
        jobsRunningOnThisThread.remove();
        future.whenComplete((result, failure) -> signalWaiters());
        boolean interrupted = false;
        while (true) {
            // always clear the interrupt, so that the wait below blocks
            if (Thread.interrupted() && !interrupted) {
                interrupted = true;
                // jobs that check the future stop early; the rest still run, since what they use is about to be
                // released
                future.cancel(false);
            }
            final Job job = claimNextPendingJob();
            if (job != null) {
                runOnWaitingThread(job, future);
                continue;
            }
            synchronized (outstandingJobs) {
                if (future.isDone() && outstandingJobs.get() == 0) {
                    break;
                }
                if (pendingJobs.isEmpty()) {
                    try {
                        outstandingJobs.wait();
                    } catch (InterruptedException e) {
                        // handled at the top of the loop
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        if (interrupted) {
            throw new InterruptedException();
        }
        return future.get();
    }

    /**
     * Run a job this waiting thread has claimed. It ends as it would on an executor thread: a failure reaches the job's
     * own error handler, and an Error, or a failure of the handler itself, is also reported as fatal. Nothing the job
     * throws escapes the wait; it fails {@code future} instead, unless the job's handler has already completed it, so
     * that a handler that failed before completing it does not leave the wait with nothing to wait for.
     */
    private static void runOnWaitingThread(@NotNull final Job job, @NotNull final CompletableFuture<?> future) {
        try {
            job.runClaimed(false);
        } catch (Throwable t) {
            // JobScheduler.runJob reported this as fatal before rethrowing it: an Error from the job, or a failure of
            // its error handler.
            future.completeExceptionally(t);
        }
    }

    /**
     * @return a job no other thread has claimed, now claimed by this thread, or null when every job submitted has been
     *         claimed
     */
    @Nullable
    private Job claimNextPendingJob() {
        Job job;
        while ((job = pendingJobs.poll()) != null) {
            if (job.tryClaim()) {
                return job;
            }
        }
        return null;
    }

    private void signalWaiters() {
        synchronized (outstandingJobs) {
            outstandingJobs.notifyAll();
        }
    }

    /**
     * Decrement the number of outstanding jobs, either because we could not submit the job or because the job
     * completed.
     */
    private void decrementOutstandingJobs() {
        if (outstandingJobs.decrementAndGet() == 0) {
            signalWaiters();
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
        return threadCount.getAsInt();
    }

    /**
     * A submitted job, which runs once: on the executor's thread that first starts it, or on a thread waiting in
     * {@link #awaitCompletion} that claims it first. A job the submitter withdraws first does nothing when the executor
     * starts it.
     */
    private final class Job implements Runnable, Submission {

        private final Thread submitter = Thread.currentThread();
        // Whether the submitting thread is inside the executor's execute for this job, where a run is a synchronous
        // executor running the job inline; written and read only by the submitting thread.
        private boolean inExecute;
        private final AtomicBoolean claimed = new AtomicBoolean();
        // Read only by the thread that claims this job, and cleared once it has run or been withdrawn, so that an
        // entry the executor still queues for it retains none of the work.
        private ExecutionContext executionContext;
        private Runnable runnable;
        private LogOutputAppendable description;
        private Consumer<Exception> onError;
        private final boolean waitsForbidden;

        private Job(
                final ExecutionContext executionContext,
                final Runnable runnable,
                final LogOutputAppendable description,
                final Consumer<Exception> onError,
                final boolean waitsForbidden) {
            this.executionContext = executionContext;
            this.runnable = runnable;
            this.description = description;
            this.onError = onError;
            this.waitsForbidden = waitsForbidden;
        }

        /** @return whether this thread is the one to run this job */
        private boolean tryClaim() {
            return claimed.compareAndSet(false, true);
        }

        /** @return whether a thread has claimed this job to run it, or withdrawn it */
        private boolean isClaimed() {
            return claimed.get();
        }

        @Override
        public void withdraw() {
            if (!tryClaim()) {
                return;
            }
            release();
            pendingJobs.remove(this);
            decrementOutstandingJobs();
        }

        /** Drop the work, which this job will not run again. */
        private void release() {
            executionContext = null;
            runnable = null;
            description = null;
            onError = null;
        }

        @Override
        public void run() {
            if (!tryClaim()) {
                // withdrawn, or claimed by a waiting thread, before the executor started it
                return;
            }
            pendingJobs.remove(this);
            // a synchronous executor that runs it here makes it the submitting thread's own work
            runClaimed(Thread.currentThread() != submitter || !inExecute);
        }

        /**
         * Run this job, which this thread has claimed, and release its count.
         *
         * @param accumulatePerformance whether to accumulate the job's performance, as for a job that runs neither
         *        inside the executor's execute on the submitting thread nor on a thread waiting for this scheduler's
         *        jobs; a job nested inside another of this scheduler's jobs on this thread is not accumulated either
         */
        private void runClaimed(final boolean accumulatePerformance) {
            // this thread's count once it includes this job; null until then
            MutableInt jobsRunningHere = null;
            try {
                final MutableInt enclosingJobs = jobsRunningOnThisThread.get();
                if (enclosingJobs == null) {
                    final MutableInt onlyThisJob = new MutableInt(1);
                    jobsRunningOnThisThread.set(onlyThisJob);
                    jobsRunningHere = onlyThisJob;
                } else {
                    enclosingJobs.increment();
                    jobsRunningHere = enclosingJobs;
                }
                // the enclosing job's performance already holds the time of one nested in it
                final boolean accumulate = accumulatePerformance && enclosingJobs == null;
                try (final SafeCloseable ignored = waitsForbidden ? UpdateGraphWaitGuard.forbidWaits() : null) {
                    if (!accumulate) {
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
                }
            } finally {
                if (jobsRunningHere == null || jobsRunningHere.decrementAndGet() == 0) {
                    jobsRunningOnThisThread.remove();
                }
                release();
                // Decrement the outstanding jobs, even if performance accounting failed; otherwise
                // getAccumulatedPerformance would wait for the job forever.
                decrementOutstandingJobs();
            }
        }
    }
}
