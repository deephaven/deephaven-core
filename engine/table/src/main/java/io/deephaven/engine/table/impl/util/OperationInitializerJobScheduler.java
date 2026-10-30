//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraphWaitGuard;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A {@link JobScheduler} over an {@link OperationInitializer}, whose pool is shared and has a fixed number of threads.
 *
 * <p>
 * Each job runs once, on whichever thread claims it first: a pool thread, or a thread waiting in
 * {@link #awaitCompletion}, which runs the jobs no pool thread has started rather than waiting for one to. A waiting
 * thread therefore blocks only while every job it has not finished is running on another thread, and never on a job
 * queued behind pool threads that may themselves be waiting, perhaps for a lock the waiting thread holds. A job that a
 * waiting thread runs is part of that thread's own work, and its performance is not accumulated here. A job withdrawn
 * before any thread claims it never runs.
 * </p>
 */
public class OperationInitializerJobScheduler implements JobScheduler {

    private final BasePerformanceEntry accumulatedBaseEntry = new BasePerformanceEntry();
    private final OperationInitializer operationInitializer;
    private final ThreadLocal<BasePerformanceEntry> currentBaseEntry = new ThreadLocal<>();
    /**
     * The jobs submitted that have not finished or been withdrawn, whichever thread runs them. Its monitor guards the
     * waits for jobs, and is notified when a job is submitted, when the count reaches zero, and when a future being
     * waited for completes.
     */
    private final AtomicInteger outstandingJobs = new AtomicInteger(0);
    /**
     * The jobs submitted, in order, for a waiting thread to claim. A job a pool thread claims or the submitter
     * withdraws is removed; any other claimed job is discarded when a waiting thread polls it.
     */
    private final Queue<Job> pendingJobs = new ConcurrentLinkedQueue<>();

    public OperationInitializerJobScheduler(@NotNull final OperationInitializer operationInitializer) {
        this.operationInitializer = operationInitializer;
    }

    public OperationInitializerJobScheduler() {
        this(ExecutionContext.getContext().getOperationInitializer());
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
        final Job job = new Job(executionContext, runnable, description, onError, waitsForbidden);
        outstandingJobs.incrementAndGet();
        pendingJobs.add(job);
        signalWaiters();
        try {
            operationInitializer.submit(job::runFromInitializer);
        } catch (Throwable t) {
            // A job that never started is withdrawn here, releasing its count, or getAccumulatedPerformance would wait
            // forever for it; an Error counts too, OutOfMemoryError when the pool cannot make a thread in practice. A
            // job that an inline initializer or a waiting thread claimed releases its count itself, whatever it threw.
            job.withdraw();
            throw t;
        }
        return job;
    }

    @Override
    public <T> T awaitCompletion(@NotNull final CompletableFuture<T> future)
            throws InterruptedException, ExecutionException {
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
                runOnWaitingThread(job);
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
     * Run a job this waiting thread has claimed. It ends as it would on a pool thread: a failure reaches the job's own
     * error handler, and an Error is also reported as fatal, so the future being waited for reports the outcome and
     * nothing the job throws escapes the wait.
     */
    private static void runOnWaitingThread(@NotNull final Job job) {
        try {
            job.run(false);
        } catch (Throwable t) {
            // delivered and reported by JobScheduler.runJob, as on a pool thread
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
     * Run the given job, under the provided ExecutionContext; recording performance into our basePerformanceEntry
     * (unless we are being dispatched as a sub-job to avoid double counting).
     *
     * @param executionContext the ExecutionContext
     * @param runnable the runnable to run
     * @param description a description of the runnable for error messages
     * @param onError a Consumer to call if an Exception occurs
     */
    private void wrapRunnable(final ExecutionContext executionContext,
            final Runnable runnable,
            final LogOutputAppendable description,
            final Consumer<Exception> onError) {
        final BasePerformanceEntry basePerformanceEntry;
        if (currentBaseEntry.get() == null) {
            basePerformanceEntry = new BasePerformanceEntry();
            basePerformanceEntry.onBaseEntryStart();
            currentBaseEntry.set(basePerformanceEntry);
        } else {
            basePerformanceEntry = null;
        }
        try {
            JobScheduler.runJob(executionContext, runnable, description, onError);
        } finally {
            if (basePerformanceEntry != null) {
                Assert.equals(currentBaseEntry.get(), "currentBaseEntry.get()", basePerformanceEntry,
                        "basePerformanceEntry");
                currentBaseEntry.remove();
                basePerformanceEntry.onBaseEntryEnd();
                accumulatedBaseEntry.accumulate(basePerformanceEntry);
            }
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
        return operationInitializer.parallelismFactor();
    }

    /**
     * A submitted job, which runs once, on the first thread to claim it.
     */
    private final class Job implements Submission {

        private final ExecutionContext executionContext;
        private final Runnable runnable;
        private final LogOutputAppendable description;
        private final Consumer<Exception> onError;
        private final boolean waitsForbidden;
        private final AtomicBoolean claimed = new AtomicBoolean();

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

        @Override
        public boolean withdraw() {
            if (!tryClaim()) {
                return false;
            }
            pendingJobs.remove(this);
            decrementOutstandingJobs();
            return true;
        }

        /** What the operation initializer runs: this job, unless another thread has claimed or withdrawn it. */
        private void runFromInitializer() {
            if (!tryClaim()) {
                return;
            }
            pendingJobs.remove(this);
            run(true);
        }

        /**
         * Run this job, which this thread has claimed, and release its count.
         *
         * @param accumulatePerformance whether to accumulate the job's performance, as for a job that does not run on a
         *        thread waiting for this scheduler's jobs
         */
        private void run(final boolean accumulatePerformance) {
            try (final SafeCloseable ignored = waitsForbidden ? UpdateGraphWaitGuard.forbidWaits() : null) {
                if (accumulatePerformance) {
                    wrapRunnable(executionContext, runnable, description, onError);
                } else {
                    JobScheduler.runJob(executionContext, runnable, description, onError);
                }
            } finally {
                // even if the performance accounting failed, or getAccumulatedPerformance would wait for this job
                // forever
                decrementOutstandingJobs();
            }
        }
    }
}
