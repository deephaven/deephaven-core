/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

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
