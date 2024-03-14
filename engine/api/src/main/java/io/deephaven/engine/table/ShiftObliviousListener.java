//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.rowset.RowSet;

/**
 * Shift-oblivious listener for table changes.
 */
public interface ShiftObliviousListener extends TableListener {

    /**
     * Process notification of table changes.
     *
     * <p>
     * The ShiftObliviousListener onUpdate call executes within the {@link io.deephaven.engine.updategraph.UpdateGraph}
     * refresh loop. Any tables used within the listener's onUpdate call must have already been refreshed. Using the
     * typical pattern of a Listener that is listening to a single table, with
     * {@link Table#addUpdateListener(ShiftObliviousListener)}, this is trivially true.
     * </p>
     *
     * <p>
     * When the listener must reference more than just one parent, the tables (or other objects) it references, must be
     * made a {@link NotificationQueue.Dependency} of the listener. For listeners that reference multiple ticking
     * tables, a common pattern is to use a MergedListener and collection of ListenerRecorders.
     * </p>
     *
     *
     * @param added rows added
     * @param removed rows removed
     * @param modified rows modified
     */
    void onUpdate(RowSet added, RowSet removed, RowSet modified);

    /**
     * Creates a notification for the table changes.
     *
     * @param added rows added
     * @param removed rows removed
     * @param modified rows modified
     * @return table change notification
     */
    NotificationQueue.Notification getNotification(RowSet added, RowSet removed, RowSet modified);

    /**
     * Sets the RowSet for the initial data.
     *
     * @param initialImage initial image
     */
    void setInitialImage(RowSet initialImage);
}
