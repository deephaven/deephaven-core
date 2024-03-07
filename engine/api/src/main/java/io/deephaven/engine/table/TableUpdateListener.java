//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.engine.updategraph.NotificationQueue;

/**
 * Shift-aware listener for table changes.
 */
public interface TableUpdateListener extends TableListener {

    /**
     * Process notification of table changes.
     *
     * <p>
     * The TableUpdateListener onUpdate call executes within the {@link io.deephaven.engine.updategraph.UpdateGraph}
     * refresh loop. Any tables used within the listener's onUpdate call must have already been refreshed. Using the
     * typical pattern of a Listener that is listening to a single table, with
     * {@link Table#addUpdateListener(TableUpdateListener)}, this is trivially true.
     * </p>
     *
     * <p>
     * When the listener must reference more than just one parent, the tables (or other objects) it references, must be
     * made a {@link NotificationQueue.Dependency} of the listener. For listeners that reference multiple ticking
     * tables, a common pattern is to use a MergedListener and collection of ListenerRecorders.
     * </p>
     *
     * @param upstream The set of upstream table updates.
     */
    void onUpdate(TableUpdate upstream);

    /**
     * Creates a notification for the table changes.
     *
     * @param upstream The set of upstream table updates.
     * @return table change notification
     */
    NotificationQueue.Notification getNotification(TableUpdate upstream);
}
