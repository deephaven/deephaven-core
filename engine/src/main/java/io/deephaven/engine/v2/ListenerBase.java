/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2;

import io.deephaven.engine.tables.live.NotificationQueue;
import io.deephaven.engine.util.liveness.LivenessNode;
import io.deephaven.engine.v2.utils.UpdatePerformanceTracker;

/**
 * Listener for table changes.
 */
public interface ListenerBase extends LivenessNode {
    /**
     * Notification of exceptions.
     *
     * @param originalException exception
     * @param sourceEntry performance tracking
     */
    void onFailure(Throwable originalException, UpdatePerformanceTracker.Entry sourceEntry);

    /**
     * Creates a notification for the exception.
     *
     * @param originalException exception
     * @param sourceEntry performance tracking
     * @return exception notification
     */
    NotificationQueue.Notification getErrorNotification(Throwable originalException,
            UpdatePerformanceTracker.Entry sourceEntry);
}
