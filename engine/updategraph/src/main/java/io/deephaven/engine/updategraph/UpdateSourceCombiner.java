package io.deephaven.engine.updategraph;

import io.deephaven.base.WeakReferenceManager;
import io.deephaven.engine.liveness.LivenessArtifact;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;

/**
 * Update source that combines multiple sources in order to force them to be refreshed as a unit within the
 * {@link UpdateGraphProcessor#DEFAULT update graph processor}.
 */
public class UpdateSourceCombiner extends LivenessArtifact implements Runnable, UpdateSourceRegistrar {

    private final WeakReferenceManager<Runnable> combinedTables = new WeakReferenceManager<>(true);

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
     * Passes through to the {@link UpdateGraphProcessor#DEFAULT update graph processor}.
     */
    @Override
    public void requestRefresh() {
        UpdateGraphProcessor.DEFAULT.requestRefresh();
    }

    @Override
    public void destroy() {
        super.destroy();
        UpdateGraphProcessor.DEFAULT.removeSource(this);
    }
}
