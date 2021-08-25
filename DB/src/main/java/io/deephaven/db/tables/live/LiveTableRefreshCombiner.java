package io.deephaven.db.tables.live;

import io.deephaven.base.WeakReferenceManager;
import io.deephaven.db.util.liveness.LivenessArtifact;
import io.deephaven.db.v2.DynamicNode;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;

/**
 * Combines multiple {@link LiveTable}s into a single one, in order to allow for update parallelization within the
 * {@link LiveTableMonitor}.
 */
public class LiveTableRefreshCombiner extends LivenessArtifact implements LiveTable, LiveTableRegistrar {

    private final WeakReferenceManager<LiveTable> combinedTables = new WeakReferenceManager<>(true);

    @Override
    public void refresh() {
        combinedTables.forEachValidReference(LiveTable::refresh);
    }

    @Override
    public void addTable(@NotNull final LiveTable liveTable) {
        if (liveTable instanceof DynamicNode) {
            final DynamicNode dynamicLiveTable = (DynamicNode) liveTable;
            // Like a LiveTableMonitor, we need to ensure that DynamicNodes added to this combiner are set to refresh.
            // NB: addParentReference usually sets refreshing as a side effect, but it's clearer to do it explicitly.
            dynamicLiveTable.setRefreshing(true);
            // Unlike a LiveTableMonitor, we must also ensure that DynamicNodes added to this combiner have the
            // combiner as a parent, in order to ensure the integrity of the resulting DAG.
            dynamicLiveTable.addParentReference(this);
        }
        combinedTables.add(liveTable);
    }

    @Override
    public void removeTable(@NotNull final LiveTable liveTable) {
        combinedTables.removeAll(Collections.singleton(liveTable));
    }

    @Override
    public void requestRefresh(@NotNull LiveTable table) {
        LiveTableMonitor.DEFAULT.requestRefresh(table);
    }

    @Override
    public void maybeRefreshTable(@NotNull LiveTable table, boolean onlyIfHaveLock) {
        LiveTableMonitor.DEFAULT.maybeRefreshTable(table, onlyIfHaveLock);
    }

    @Override
    public void destroy() {
        super.destroy();
        LiveTableMonitor.DEFAULT.removeTable(this);
    }
}
