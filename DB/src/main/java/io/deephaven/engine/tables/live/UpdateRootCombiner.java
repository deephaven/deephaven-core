package io.deephaven.engine.tables.live;

import io.deephaven.base.WeakReferenceManager;
import io.deephaven.engine.util.liveness.LivenessArtifact;
import io.deephaven.engine.v2.DynamicNode;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;

/**
 * Combines multiple {@link LiveTable}s into a single one, in order to allow for update parallelization within the
 * {@link UpdateGraphProcessor}.
 */
public class UpdateRootCombiner extends LivenessArtifact implements LiveTable, UpdateRootRegistrar {

    private final WeakReferenceManager<Runnable> combinedTables = new WeakReferenceManager<>(true);

    @Override
    public void run() {
        combinedTables.forEachValidReference(Runnable::run);
    }

    @Override
    public void addTable(@NotNull final Runnable updateRoot) {
        if (updateRoot instanceof DynamicNode) {
            final DynamicNode dynamicLiveTable = (DynamicNode) updateRoot;
            // Like a UpdateGraphProcessor, we need to ensure that DynamicNodes added to this combiner are set to run.
            // NB: addParentReference usually sets refreshing as a side effect, but it's clearer to do it explicitly.
            dynamicLiveTable.setRefreshing(true);
            // Unlike a UpdateGraphProcessor, we must also ensure that DynamicNodes added to this combiner have the
            // combiner as a parent, in order to ensure the integrity of the resulting DAG.
            dynamicLiveTable.addParentReference(this);
        }
        combinedTables.add(updateRoot);
    }

    @Override
    public void removeTable(@NotNull final Runnable updateRoot) {
        combinedTables.removeAll(Collections.singleton(updateRoot));
    }

    @Override
    public void requestRefresh(@NotNull Runnable table) {
        UpdateGraphProcessor.DEFAULT.requestRefresh(table);
    }

    @Override
    public void maybeRefreshTable(@NotNull Runnable table, boolean onlyIfHaveLock) {
        UpdateGraphProcessor.DEFAULT.maybeRefreshTable(table, onlyIfHaveLock);
    }

    @Override
    public void destroy() {
        super.destroy();
        UpdateGraphProcessor.DEFAULT.removeTable(this);
    }
}
