/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.live.NotificationQueue;

/**
 * Common base class for index update notifications.
 */
public abstract class AbstractIndexUpdateNotification extends AbstractNotification
    implements NotificationQueue.IndexUpdateNotification {

    protected AbstractIndexUpdateNotification(final boolean isTerminal) {
        super(isTerminal);
    }
}
