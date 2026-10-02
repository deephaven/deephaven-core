//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.engine.updategraph.UpdateGraph;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

public class UpdateGraphJobScheduler implements JobScheduler {
    final BasePerformanceEntry accumulatedBaseEntry = new BasePerformanceEntry();

    private final UpdateGraph updateGraph;

    public UpdateGraphJobScheduler(@NotNull final UpdateGraph updateGraph) {
        this.updateGraph = updateGraph;
    }

    public UpdateGraphJobScheduler() {
        this(ExecutionContext.getContext().getUpdateGraph());
    }

    @Override
    public void submit(
            final ExecutionContext executionContext,
            final Runnable runnable,
            final LogOutputAppendable description,
            final Consumer<Exception> onError) {
        updateGraph.addNotification(new AbstractNotification(false) {
            @Override
            public boolean canExecute(long step) {
                return true;
            }

            @Override
            public void run() {
                final BasePerformanceEntry baseEntry = new BasePerformanceEntry();
                baseEntry.onBaseEntryStart();
                try {
                    JobScheduler.runJob(executionContext, runnable, description, onError);
                } finally {
                    baseEntry.onBaseEntryEnd();
                    accumulatedBaseEntry.accumulate(baseEntry);
                }
            }

            @Override
            public LogOutput append(LogOutput output) {
                return output.append("{Notification(").append(System.identityHashCode(this)).append(" for ")
                        .append(description).append("}");
            }
        });
    }

    @Override
    public BasePerformanceEntry getAccumulatedPerformance() {
        return accumulatedBaseEntry;
    }

    @Override
    public int threadCount() {
        return updateGraph.parallelismFactor();
    }

    /**
     * Jobs submitted here run as notifications on the update graph's own threads. A thread that blocked on this
     * scheduler could be one of them, and if every update thread blocked this way the notifications they wait for could
     * never run; so no thread may block on it.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void checkInvokeSupported() {
        throw new UnsupportedOperationException("A thread cannot block on the update graph's job scheduler: its jobs "
                + "run as notifications on the update graph's own threads, which may be the ones waiting. Use "
                + "iterateParallel or iterateSerial and continue from its completion callback instead.");
    }
}
