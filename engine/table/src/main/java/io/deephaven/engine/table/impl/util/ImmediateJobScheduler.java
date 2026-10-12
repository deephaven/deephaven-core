//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.util.SafeCloseable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

/**
 * A {@link JobScheduler} that runs jobs on the thread that submits them, with a {@link #threadCount() thread count} of
 * one.
 * <p>
 * A {@link #submit submit} from a thread that is not already running this scheduler's jobs runs the job before
 * returning, along with every job submitted while it runs. Jobs submitted from within a running job are queued rather
 * than run recursively, and the queue is drained most recently submitted first, so nested work runs depth-first rather
 * than in submission order.
 * <p>
 * Only one thread may run this scheduler's jobs at a time: a submit from any other thread while jobs are running throws
 * {@link IllegalCallerException}. A job that completes asynchronously must therefore resume on the running thread, or
 * after the jobs have finished. An {@link Error} thrown by a job propagates out of the submit that is running it.
 */
public class ImmediateJobScheduler implements JobScheduler {

    private volatile Thread processingThread;
    private static final AtomicReferenceFieldUpdater<ImmediateJobScheduler, Thread> PROCESSING_THREAD_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ImmediateJobScheduler.class, Thread.class, "processingThread");

    private final Deque<Runnable> pendingJobs = new ArrayDeque<>();

    @Override
    public void submit(
            final ExecutionContext executionContext,
            final Runnable runnable,
            final LogOutputAppendable description,
            final Consumer<Exception> onError) {
        final Thread thisThread = Thread.currentThread();
        final boolean thisThreadIsProcessing = processingThread == thisThread;

        if (!thisThreadIsProcessing && !PROCESSING_THREAD_UPDATER.compareAndSet(this, null, thisThread)) {
            throw new IllegalCallerException("An unexpected thread submitted a job to this job scheduler");
        }

        pendingJobs.addLast(() -> {
            // We do not need to install the update context since we are not changing thread contexts.
            try (SafeCloseable ignored = executionContext != null ? executionContext.open() : null) {
                runnable.run();
            } catch (Exception e) {
                onError.accept(e);
            }
        });

        if (thisThreadIsProcessing) {
            // We're already draining the queue in an ancestor stack frame
            return;
        }

        try {
            Runnable job;
            while ((job = pendingJobs.pollLast()) != null) {
                job.run();
            }
        } finally {
            PROCESSING_THREAD_UPDATER.set(this, null);
        }
    }

    @Override
    public BasePerformanceEntry getAccumulatedPerformance() {
        return null;
    }

    @Override
    public int threadCount() {
        return 1;
    }
}
