//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
