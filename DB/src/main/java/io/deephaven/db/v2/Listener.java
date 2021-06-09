/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.v2.utils.Index;

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
    void onUpdate(Index added, Index removed, Index modified);

    /**
     * Creates a notification for the table changes.
     *
     * @param added rows added
     * @param removed rows removed
     * @param modified rows modified
     * @return table change notification
     */
    NotificationQueue.IndexUpdateNotification getNotification(Index added, Index removed, Index modified);

    /**
     * Sets the index for the initial data.
     *
     * @param initialImage initial image
     */
    void setInitialImage(Index initialImage);
}
