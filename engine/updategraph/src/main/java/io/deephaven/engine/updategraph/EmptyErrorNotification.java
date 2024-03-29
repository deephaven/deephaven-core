//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

/**
 * A {@link NotificationQueue.ErrorNotification} that does not actually notify anything.
 */
public class EmptyErrorNotification extends EmptyNotification implements NotificationQueue.ErrorNotification {
}
