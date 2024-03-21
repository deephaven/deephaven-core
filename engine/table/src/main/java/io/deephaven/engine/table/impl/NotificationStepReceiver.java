//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

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

    /**
     * Deliver the appropriate last notification step to a receiver that isn't derived from a
     * {@link NotificationStepSource}.
     */
    @FinalDefault
    default void initializeLastNotificationStep(@NotNull final LogicalClock clock) {
        final long currentClockValue = clock.currentValue();
        setLastNotificationStep(LogicalClock.getState(currentClockValue) == LogicalClock.State.Updating
                ? LogicalClock.getStep(currentClockValue) - 1
                : LogicalClock.getStep(currentClockValue));
    }
}
