package io.deephaven.engine.updategraph.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.logger.Logger;
import io.deephaven.net.CommBase;
import org.jetbrains.annotations.NotNull;

/**
 * An EventDrivenUpdateGraph provides an isolated refresh processor.
 *
 * As with a {@link PeriodicUpdateGraph}, the EventDrivenUpdateGraph contains a set of sources, but it is refreshed only
 * when a call to {@link #requestRefresh()} is made. All sources are synchronously refreshed on that thread; and then
 * the resultant notifications are also synchronously processed.
 */
public class EventDrivenUpdateGraph extends AbstractUpdateGraph {
    private static final Logger log = LoggerFactory.getLogger(EventDrivenUpdateGraph.class);

    public EventDrivenUpdateGraph(String name, long minimumCycleDurationToLogNanos) {
        super(name, false, log, minimumCycleDurationToLogNanos);
        notificationProcessor = new QueueNotificationProcessor();
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("EventDrivenUpdateGraph-").append(getName());
    }

    @Override
    protected LogEntry logCycleExtra(LogEntry entry) {
        return entry;
    }

    @Override
    public int parallelismFactor() {
        return 1;
    }

    @Override
    public void requestRefresh() {
        // do the work to refresh everything, on this thread
        isUpdateThread.set(true);
        try {
            refreshAllTables();
        } finally {
            isUpdateThread.remove();
        }
        final long now = CommBase.getScheduler().currentTimeMillis();
        checkUpdatePerformanceFlush(now, now);
    }
}
