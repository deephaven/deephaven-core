package io.deephaven.engine.updategraph;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Update source that combines multiple sources. Each registered source is invoked exactly once on the next
 * {@link #run()} and then automatically de-registered.
 */
public class OneShotUpdateCombiner implements Runnable, UpdateSourceRegistrar {

    private final Queue<Runnable> sources = new ArrayDeque<>();

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
     * Passes through to the {@link UpdateGraphProcessor#DEFAULT update graph processor}.
     */
    @Override
    public void requestRefresh() {
        UpdateGraphProcessor.DEFAULT.requestRefresh();
    }
}
