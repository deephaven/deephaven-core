package io.deephaven.engine.updategraph;

import io.deephaven.base.log.LogOutput;

/**
 * A {@link NotificationQueue.Notification} that does not actually notify anything.
 */
public class EmptyNotification extends AbstractNotification {

    public EmptyNotification() {
        super(false);
    }

    @Override
    public boolean isTerminal() {
        return false;
    }

    @Override
    public boolean canExecute(final long step) {
        // Since this literally does nothing, it's safe to execute anytime.
        return true;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append(getClass().toString());
    }

    @Override
    public final void run() {}
}
