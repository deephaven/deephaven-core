/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.engine.tables.live.NotificationQueue;

/**
 * Common base class for rowSet update notifications.
 */
public abstract class AbstractIndexUpdateNotification extends AbstractNotification
        implements NotificationQueue.IndexUpdateNotification {

    protected AbstractIndexUpdateNotification(final boolean isTerminal) {
        super(isTerminal);
    }
}
