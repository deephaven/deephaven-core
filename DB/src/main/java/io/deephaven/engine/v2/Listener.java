/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2;

import io.deephaven.engine.tables.live.NotificationQueue;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;

/**
 * Listener for table changes.
 */
public interface Listener extends ListenerBase {
    /**
     * Process notification of table changes.
     *
     * @param added rows added
     * @param removed rows removed
     * @param modified rows modified
     */
    void onUpdate(TrackingMutableRowSet added, TrackingMutableRowSet removed, TrackingMutableRowSet modified);

    /**
     * Creates a notification for the table changes.
     *
     * @param added rows added
     * @param removed rows removed
     * @param modified rows modified
     * @return table change notification
     */
    NotificationQueue.IndexUpdateNotification getNotification(TrackingMutableRowSet added, TrackingMutableRowSet removed, TrackingMutableRowSet modified);

    /**
     * Sets the rowSet for the initial data.
     *
     * @param initialImage initial image
     */
    void setInitialImage(TrackingMutableRowSet initialImage);
}
