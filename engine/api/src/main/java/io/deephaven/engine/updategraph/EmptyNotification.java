package io.deephaven.engine.updategraph;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.v2.ShiftObliviousSwapListener;
import io.deephaven.engine.v2.utils.AbstractNotification;

/**
 * This is a notification that does not actually notify anything.
 *
 * It is useful for the {@link ShiftObliviousSwapListener} to have the ability to create a notification for its parent
 * before there is anything to notify.
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
