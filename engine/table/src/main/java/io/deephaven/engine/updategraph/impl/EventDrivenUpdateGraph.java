package io.deephaven.engine.updategraph.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

/**
 * An EventDrivenUpdateGraph provides an isolated refresh processor.
 *
 * <p>
 * As with a {@link PeriodicUpdateGraph}, the EventDrivenUpdateGraph contains a set of sources, but it is refreshed only
 * when a call to {@link #requestRefresh()} is made. All sources are synchronously refreshed on that thread, and then
 * the resultant notifications are also synchronously processed.
 * </p>
 */
public class EventDrivenUpdateGraph extends BaseUpdateGraph {
    private static final Logger log = LoggerFactory.getLogger(EventDrivenUpdateGraph.class);
    private boolean started = false;

    /**
     * Create a builder for an EventDrivenUpdateGraph with the given name.
     *
     * @param name the name of the new EventDrivenUpdateGraph
     * @return a builder for the EventDrivenUpdateGraph
     */
    public static EventDrivenUpdateGraph.Builder newBuilder(final String name) {
        return new EventDrivenUpdateGraph.Builder(name);
    }

    private EventDrivenUpdateGraph(String name, long minimumCycleDurationToLogNanos) {
        super(name, false, log, minimumCycleDurationToLogNanos);
        notificationProcessor = new QueueNotificationProcessor();
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("EventDrivenUpdateGraph-").append(getName());
    }

    @Override
    public int parallelismFactor() {
        return 1;
    }

    /**
     * Refresh all sources and execute the resulting notifications synchronously on this thread.
     */
    @Override
    public void requestRefresh() {
        maybeStart();
        // do the work to refresh everything, on this thread
        isUpdateThread.set(true);
        try (final SafeCloseable ignored = ExecutionContext.newBuilder().setUpdateGraph(this).build().open()) {
            refreshAllTables();
        } finally {
            isUpdateThread.remove();
        }
        final long nowNanos = System.nanoTime();
        synchronized (this) {
            maybeFlushUpdatePerformance(nowNanos, nowNanos);
        }
    }

    /**
     * We defer starting the update performance tracker until our first cycle. This is essential when we are the DEFAULT
     * graph used for UPT publishing, as the UPT requires the publication graph to be in the BaseUpdateGraph map, which
     * is not done until after our constructor completes.
     */
    private synchronized void maybeStart() {
        if (started) {
            return;
        }
        updatePerformanceTracker.start();
        started = true;
    }

    @Override
    public void stop() {
        running = false;
        // if we wait for the lock to be done, then we should have completed our cycle and will not execute again
        exclusiveLock().doLocked(() -> {
        });
    }

    /**
     * Builds or retrieves a new EventDrivenUpdateGraph.
     */
    public static class Builder {
        private final String name;
        private long minimumCycleDurationToLogNanos = DEFAULT_MINIMUM_CYCLE_DURATION_TO_LOG_NANOSECONDS;

        public Builder(String name) {
            this.name = name;
        }

        /**
         * Set the minimum duration of an update cycle that should be logged at the INFO level.
         *
         * @param minimumCycleDurationToLogNanos threshold to log a slow cycle
         * @return this builder
         */
        public Builder minimumCycleDurationToLogNanos(long minimumCycleDurationToLogNanos) {
            this.minimumCycleDurationToLogNanos = minimumCycleDurationToLogNanos;
            return this;
        }

        /**
         * Constructs and returns an EventDrivenUpdateGraph. It is an error to do so if an UpdateGraph already exists
         * with the name provided to this builder.
         *
         * @return the new EventDrivenUpdateGraph
         * @throws IllegalStateException if an UpdateGraph with the provided name already exists
         */
        public EventDrivenUpdateGraph build() {
            return BaseUpdateGraph.buildOrThrow(name, this::construct);
        }

        /**
         * Returns an existing EventDrivenUpdateGraph with the name provided to this Builder, if one exists, else
         * returns a new EventDrivenUpdateGraph.
         *
         * <p>
         * If the options for the existing graph are different than the options specified in this Builder, this
         * Builder's options are ignored.
         * </p>
         *
         * @return the EventDrivenUpdateGraph
         * @throws ClassCastException if the existing graph is not an EventDrivenUpdateGraph
         */
        public EventDrivenUpdateGraph existingOrBuild() {
            return BaseUpdateGraph.existingOrBuild(name, this::construct);
        }

        private EventDrivenUpdateGraph construct() {
            return new EventDrivenUpdateGraph(
                    name,
                    minimumCycleDurationToLogNanos);
        }
    }
}
