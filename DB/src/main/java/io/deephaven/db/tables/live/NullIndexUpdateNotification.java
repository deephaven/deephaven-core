package io.deephaven.db.tables.live;

import io.deephaven.base.log.LogOutput;
import io.deephaven.db.v2.utils.AbstractIndexUpdateNotification;

/**
 * This is a notification that does not actually notify anything.
 *
 * It is useful for the {@link io.deephaven.db.v2.SwapListener} to have the ability to create a notification for its
 * parent before there is anything to notify.
 */
public class NullIndexUpdateNotification extends AbstractIndexUpdateNotification {
    public NullIndexUpdateNotification() {
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
