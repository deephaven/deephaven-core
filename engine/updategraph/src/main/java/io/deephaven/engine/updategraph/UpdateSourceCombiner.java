//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import io.deephaven.util.datastructures.ArrayWeakReferenceManager;
import io.deephaven.util.datastructures.WeakReferenceManager;
import io.deephaven.engine.liveness.LivenessArtifact;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.function.Supplier;

/**
 * Update source that combines multiple sources in order to force them to be refreshed as a unit within the
 * {@link UpdateGraph update graph} provided at construction.
 */
public class UpdateSourceCombiner extends LivenessArtifact implements Runnable, UpdateSourceRegistrar {

    private final UpdateGraph updateGraph;
    private final boolean parallel;
    private final WeakReferenceManager<Runnable> sources;

    /**
     * Construct an UpdateSourceCombiner with default parameters. The result will process updates serially, and will use
     * a concurrent {@link ArrayWeakReferenceManager}.
     *
     * @param updateGraph The {@link UpdateGraph} to register with
     */
    public UpdateSourceCombiner(@NotNull final UpdateGraph updateGraph) {
        this(updateGraph, false, ArrayWeakReferenceManager::new);
    }

    /**
     * Construct an UpdateSourceCombiner.
     * 
     * @param updateGraph The {@link UpdateGraph} to register with
     * @param parallel Whether to process updates in parallel
     * @param weakReferenceManagerFactory Factory for the {@link WeakReferenceManager} to use when holding sources
     */
    public UpdateSourceCombiner(
            @NotNull final UpdateGraph updateGraph,
            final boolean parallel,
            @NotNull final Supplier<WeakReferenceManager<Runnable>> weakReferenceManagerFactory) {
        this.updateGraph = updateGraph;
        this.parallel = parallel;
        this.sources = weakReferenceManagerFactory.get();
    }

    /**
     * Add this UpdateSourceCombiner to the {@link UpdateGraph update graph} passed at construction. This should only be
     * done once.
     */
    public void install() {
        updateGraph.addSource(this);
    }

    @Override
    public void run() {
        sources.forEachValidReference(Runnable::run, parallel);
    }

    @Override
    public void addSource(@NotNull final Runnable updateSource) {
        if (updateSource instanceof DynamicNode) {
            final DynamicNode dynamicUpdateSource = (DynamicNode) updateSource;
            // Like a UpdateGraph, we need to ensure that DynamicNodes added to this combiner are set to
            // refreshing.
            // NB: addParentReference usually sets refreshing as a side effect, but it's clearer to do it explicitly.
            dynamicUpdateSource.setRefreshing(true);
            // Unlike an UpdateGraph, we must also ensure that DynamicNodes added to this combiner have the
            // combiner as a parent, in order to ensure the integrity of the resulting DAG.
            dynamicUpdateSource.addParentReference(this);
        }
        sources.add(updateSource);
    }

    @Override
    public void removeSource(@NotNull final Runnable updateSource) {
        sources.remove(updateSource);
    }

    /**
     * Passes through to the {@link UpdateGraph update graph} passed at construction.
     */
    @Override
    public void requestRefresh() {
        updateGraph.requestRefresh();
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    public void destroy() {
        super.destroy();
        updateGraph.removeSource(this);
    }

    @Override
    public boolean satisfied(final long step) {
        return updateGraph.satisfied(step);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }
}
