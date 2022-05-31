/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table;

import io.deephaven.engine.updategraph.NotificationQueue;

/**
 * Shift-aware listener for table changes.
 */
public interface TableUpdateListener extends TableListener {

    /**
     * Process notification of table changes.
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
