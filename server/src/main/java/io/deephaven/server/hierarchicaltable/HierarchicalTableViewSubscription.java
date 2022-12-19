package io.deephaven.server.hierarchicaltable;

import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.server.util.Scheduler;
import org.jetbrains.annotations.NotNull;

import java.util.BitSet;

/**
 * Tool that manages an active subscription to a {@link HierarchicalTableView}.
 */
public class HierarchicalTableViewSubscription extends LivenessArtifact {

    private final Scheduler scheduler;

    private final HierarchicalTableView view;
    private final long intervalMilliseconds;

    private final Thread snapshotSender;

    private BitSet columns;
    private RowSet rows;

    public HierarchicalTableViewSubscription(
            @NotNull final HierarchicalTableView view,
            final long intervalMilliseconds) {
        this.view = view;
        this.intervalMilliseconds = intervalMilliseconds;
        snapshotSender = new Thread(this::sendSnapshot, "HierarchicalTableView-" + view + "-snapshotSender");
    }

    private void sendSnapshot() {
        view.getHierarchicalTable().snapshot(view.getSnapshotState(), view.getKeyTable(), view.getKeyTableActionColumn(), columns, rows, );
    }
}
