/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.updategraph;

import io.deephaven.base.log.LogOutput;

/**
 * A {@link NotificationQueue.ErrorNotification} that does not actually notify anything.
 */
public class EmptyErrorNotification extends EmptyNotification implements NotificationQueue.ErrorNotification {
}
