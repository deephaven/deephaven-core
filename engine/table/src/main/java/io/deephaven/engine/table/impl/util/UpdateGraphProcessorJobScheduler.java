package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.process.ProcessEnvironment;

import java.util.function.Consumer;

public class UpdateGraphProcessorJobScheduler implements JobScheduler {
    final BasePerformanceEntry accumulatedBaseEntry = new BasePerformanceEntry();

    @Override
    public void submit(
            final ExecutionContext executionContext,
            final Runnable runnable,
            final LogOutputAppendable description,
            final Consumer<Exception> onError) {
        UpdateGraphProcessor.DEFAULT.addNotification(new AbstractNotification(false) {
            @Override
            public boolean canExecute(long step) {
                return true;
            }

            @Override
            public void run() {
                final BasePerformanceEntry baseEntry = new BasePerformanceEntry();
                baseEntry.onBaseEntryStart();
                try {
                    runnable.run();
                } catch (Exception e) {
                    onError.accept(e);
                } catch (Error e) {
                    final String logMessage = new LogOutputStringImpl().append(description).append(" Error").toString();
                    ProcessEnvironment.getGlobalFatalErrorReporter().report(logMessage, e);
                    throw e;
                } finally {
                    baseEntry.onBaseEntryEnd();
                    synchronized (accumulatedBaseEntry) {
                        accumulatedBaseEntry.accumulate(baseEntry);
                    }
                }
            }

            @Override
            public LogOutput append(LogOutput output) {
                return output.append("{Notification(").append(System.identityHashCode(this)).append(" for ")
                        .append(description).append("}");
            }

            @Override
            public ExecutionContext getExecutionContext() {
                return executionContext;
            }
        });
    }

    @Override
    public BasePerformanceEntry getAccumulatedPerformance() {
        return accumulatedBaseEntry;
    }

    @Override
    public int threadCount() {
        return UpdateGraphProcessor.DEFAULT.getUpdateThreads();
    }
}
