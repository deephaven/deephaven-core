//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.process.ProcessEnvironment;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class OperationInitializerJobScheduler implements JobScheduler {

    private final BasePerformanceEntry accumulatedBaseEntry = new BasePerformanceEntry();
    private final OperationInitializer operationInitializer;
    private final ThreadLocal<BasePerformanceEntry> currentBaseEntry = new ThreadLocal<>();
    private final AtomicInteger outstandingJobs =  new AtomicInteger(0);

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
        operationInitializer.submit(() -> {
            final BasePerformanceEntry basePerformanceEntry;
            if (currentBaseEntry.get() == null) {
                basePerformanceEntry = new BasePerformanceEntry();
                basePerformanceEntry.onBaseEntryStart();
                currentBaseEntry.set(basePerformanceEntry);
            } else {
                basePerformanceEntry = null;
            }
            try (final SafeCloseable ignored = executionContext == null ? null : executionContext.open()) {
                runnable.run();
            } catch (Exception e) {
                onError.accept(e);
            } catch (Error e) {
                final String logMessage = new LogOutputStringImpl().append(description).append(" Error").toString();
                ProcessEnvironment.getGlobalFatalErrorReporter().report(logMessage, e);
                throw e;
            } finally {
                if (basePerformanceEntry != null) {
                    Assert.equals(currentBaseEntry.get(), "currentBaseEntry.get()", basePerformanceEntry, "basePerformancEntry");
                    currentBaseEntry.remove();
                    basePerformanceEntry.onBaseEntryEnd();
                    accumulatedBaseEntry.accumulate(basePerformanceEntry);
                }
                if (outstandingJobs.decrementAndGet() == 0) {
                    synchronized (outstandingJobs) {
                        outstandingJobs.notifyAll();
                    }
                }
            }
        });
    }

    @Override
    public BasePerformanceEntry getAccumulatedPerformance() {
        synchronized (outstandingJobs) {
            while (outstandingJobs.get() > 0) {
                try {
                    outstandingJobs.wait();
                } catch (InterruptedException ignored) {}
            }
        }
        return accumulatedBaseEntry;
    }

    @Override
    public int threadCount() {
        return operationInitializer.parallelismFactor();
    }
}
