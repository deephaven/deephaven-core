/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.updategraph;

import io.deephaven.base.WeakReferenceManager;
import io.deephaven.engine.liveness.LivenessArtifact;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;

/**
 * Update source that combines multiple sources in order to force them to be refreshed as a unit within the
 * {@link UpdateGraphProcessor update graph processor} registered in the update context at the time of construction.
 */
public class UpdateSourceCombiner extends LivenessArtifact implements Runnable, UpdateSourceRegistrar {

    private final UpdateContext updateContext;

    private final WeakReferenceManager<Runnable> combinedTables = new WeakReferenceManager<>(true);

    public UpdateSourceCombiner() {
        updateContext = UpdateContext.get();
    }

    @Override
    public void run() {
        combinedTables.forEachValidReference(Runnable::run);
    }

    @Override
    public void addSource(@NotNull final Runnable updateSource) {
        if (updateSource instanceof DynamicNode) {
            final DynamicNode dynamicUpdateSource = (DynamicNode) updateSource;
            // Like a UpdateGraphProcessor, we need to ensure that DynamicNodes added to this combiner are set to
            // refreshing.
            // NB: addParentReference usually sets refreshing as a side effect, but it's clearer to do it explicitly.
            dynamicUpdateSource.setRefreshing(true);
            // Unlike an UpdateGraphProcessor, we must also ensure that DynamicNodes added to this combiner have the
            // combiner as a parent, in order to ensure the integrity of the resulting DAG.
            dynamicUpdateSource.addParentReference(this);
        }
        combinedTables.add(updateSource);
    }

    @Override
    public void removeSource(@NotNull final Runnable updateSource) {
        combinedTables.removeAll(Collections.singleton(updateSource));
    }

    /**
     * Passes through to the {@link UpdateGraphProcessor update graph processor} registered in the update context at
     * construction.
     */
    @Override
    public void requestRefresh() {
        updateContext.getUpdateGraphProcessor().requestRefresh();
    }

    @Override
    public void destroy() {
        super.destroy();
        updateContext.getUpdateGraphProcessor().removeSource(this);
    }
}
