/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

/**
 * Used by {@link OperationSnapshotControl} to set the notification step of elements in our DAG.
 */
public interface NotificationStepReceiver {

    /**
     * The null value for recorded notification steps.
     */
    long NULL_NOTIFICATION_STEP = -1L;

    /**
     * Deliver a last notification step to this receiver.
     *
     * @param lastNotificationStep The last notification step to be delivered
     */
    void setLastNotificationStep(long lastNotificationStep);
}
