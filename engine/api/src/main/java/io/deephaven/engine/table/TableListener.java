/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.liveness.LivenessNode;

/**
 * Listener implementation for {@link Table} changes.
 */
public interface TableListener extends LivenessNode {

    /**
     * Notification of exceptions.
     *
     * @param originalException exception
     * @param sourceEntry performance tracking
     */
    void onFailure(Throwable originalException, Entry sourceEntry);

    /**
     * Creates a notification for the exception.
     *
     * @param originalException exception
     * @param sourceEntry performance tracking
     * @return exception notification
     */
    NotificationQueue.ErrorNotification getErrorNotification(Throwable originalException, Entry sourceEntry);

    /**
     * Interface for instrumentation entries used by update graph nodes.
     */
    interface Entry extends LogOutputAppendable {
    }
}
