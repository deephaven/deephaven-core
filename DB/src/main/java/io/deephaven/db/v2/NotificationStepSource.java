package io.deephaven.db.v2;

import io.deephaven.db.tables.live.NotificationQueue;

/**
 * Elements of a Deephaven query DAG that can supply their notification step.
 */
public interface NotificationStepSource extends NotificationQueue.Dependency {

    /**
     * Get the last logical clock step on which this element dispatched a notification.
     *
     * @return The last notification step
     */
    long getLastNotificationStep();
}
