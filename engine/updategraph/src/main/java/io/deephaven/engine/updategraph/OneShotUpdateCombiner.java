//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import io.deephaven.base.log.LogOutput;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Update source that combines multiple sources. Each registered source is invoked exactly once on the next
 * {@link #run()} and then automatically de-registered.
 */
public class OneShotUpdateCombiner implements Runnable, UpdateSourceRegistrar {

    private final Queue<Runnable> sources = new ArrayDeque<>();
    private final UpdateGraph updateGraph;

    public OneShotUpdateCombiner(final UpdateGraph updateGraph) {
        this.updateGraph = updateGraph;
    }

    @Override
    public void run() {
        while (true) {
            final Runnable oneShot;
            synchronized (sources) {
                oneShot = sources.poll();
            }
            if (oneShot == null) {
                return;
            }
            oneShot.run();
        }
    }

    @Override
    public void addSource(@NotNull final Runnable updateSource) {
        synchronized (sources) {
            sources.add(updateSource);
        }
    }

    @Override
    public void removeSource(@NotNull final Runnable updateSource) {
        synchronized (sources) {
            sources.remove(updateSource);
        }
    }

    /**
     * Passes through to the {@link UpdateGraph update graph} associated with the current update context.
     */
    @Override
    public void requestRefresh() {
        updateGraph.requestRefresh();
    }

    @Override
    public boolean satisfied(final long step) {
        return updateGraph.satisfied(step);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("OneShotUpdateCombiner-").append(hashCode());
    }
}
