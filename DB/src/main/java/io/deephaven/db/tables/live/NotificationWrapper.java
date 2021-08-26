package io.deephaven.db.tables.live;

import io.deephaven.base.log.LogOutput;
import io.deephaven.db.v2.utils.AbstractNotification;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of {@link NotificationQueue.Notification} that wraps another, in order to allow overrides.
 */
public class NotificationWrapper extends AbstractNotification {

    private final NotificationQueue.Notification wrapped;

    NotificationWrapper(@NotNull final NotificationQueue.Notification wrapped) {
        super(wrapped.isTerminal());
        this.wrapped = wrapped;
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("NotificationWrapper{").append(System.identityHashCode(this)).append("} of ")
                .append(wrapped);
    }

    @Override
    public boolean canExecute(final long step) {
        return wrapped.canExecute(step);
    }

    @Override
    public void run() {
        wrapped.run();
    }
}
