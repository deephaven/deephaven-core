package io.deephaven.engine.table.impl;

import io.deephaven.engine.updategraph.NotificationQueue;

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
