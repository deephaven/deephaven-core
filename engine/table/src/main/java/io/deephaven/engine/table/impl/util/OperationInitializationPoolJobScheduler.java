package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.process.ProcessEnvironment;

import java.util.function.Consumer;

public class OperationInitializationPoolJobScheduler implements JobScheduler {
    private final BasePerformanceEntry accumulatedBaseEntry = new BasePerformanceEntry();
    private final OperationInitializer threadPool;

    public OperationInitializationPoolJobScheduler(OperationInitializer threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public void submit(
            final ExecutionContext executionContext,
            final Runnable runnable,
            final LogOutputAppendable description,
            final Consumer<Exception> onError) {
        threadPool.submit(() -> {
            final BasePerformanceEntry basePerformanceEntry = new BasePerformanceEntry();
            basePerformanceEntry.onBaseEntryStart();
            try (final SafeCloseable ignored = executionContext == null ? null : executionContext.open()) {
                runnable.run();
            } catch (Exception e) {
                onError.accept(e);
            } catch (Error e) {
                final String logMessage = new LogOutputStringImpl().append(description).append(" Error").toString();
                ProcessEnvironment.getGlobalFatalErrorReporter().report(logMessage, e);
                throw e;
            } finally {
                basePerformanceEntry.onBaseEntryEnd();
                accumulatedBaseEntry.accumulate(basePerformanceEntry);
            }
        });
    }

    @Override
    public BasePerformanceEntry getAccumulatedPerformance() {
        return accumulatedBaseEntry;
    }

    @Override
    public int threadCount() {
        return OperationInitializationThreadPool.NUM_THREADS;
    }
}
