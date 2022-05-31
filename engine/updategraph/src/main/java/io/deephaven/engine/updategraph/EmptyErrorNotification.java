package io.deephaven.engine.updategraph;

import io.deephaven.base.log.LogOutput;

/**
 * A {@link NotificationQueue.ErrorNotification} that does not actually notify anything.
 */
public class EmptyErrorNotification extends EmptyNotification implements NotificationQueue.ErrorNotification {
}
