package io.deephaven.engine.table.impl;

/**
 * Used by {@link SwapListenerBase swap listeners} to set the notification step of elements in our DAG.
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
