//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.updategraph.OperationInitializer;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class OperationInitializerJobScheduler implements JobScheduler {

    private final BasePerformanceEntry accumulatedBaseEntry = new BasePerformanceEntry();
    private final OperationInitializer operationInitializer;
    private final ThreadLocal<BasePerformanceEntry> currentBaseEntry = new ThreadLocal<>();
    private final AtomicInteger outstandingJobs = new AtomicInteger(0);

    public OperationInitializerJobScheduler(@NotNull final OperationInitializer operationInitializer) {
        this.operationInitializer = operationInitializer;
    }

    public OperationInitializerJobScheduler() {
        this(ExecutionContext.getContext().getOperationInitializer());
    }

    @Override
    public void submit(
            final ExecutionContext executionContext,
            final Runnable runnable,
            final LogOutputAppendable description,
            final Consumer<Exception> onError) {
        outstandingJobs.incrementAndGet();
        final AtomicBoolean started = new AtomicBoolean();
        try {
            operationInitializer.submit(() -> {
                started.set(true);
                wrapRunnable(executionContext, runnable, description, onError);
            });
        } catch (Throwable t) {
            // A job that never started must release its count here, or getAccumulatedPerformance would wait forever
            // for it; an Error counts too, OutOfMemoryError when the pool cannot make a thread in practice. A job that
            // an inline initializer started has released its count already, in wrapRunnable, whatever it threw.
            if (!started.get()) {
                decrementOutstandingJobs();
            }
            throw t;
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
        try {
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
        } finally {
            // even if the performance accounting failed, or getAccumulatedPerformance would wait for this job forever
            decrementOutstandingJobs();
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
        return operationInitializer.parallelismFactor();
    }
}
