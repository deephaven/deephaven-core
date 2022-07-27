/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.updategraph;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.context.ExecutionContext;

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

    @Override
    public ExecutionContext getExecutionContext() {
        return null;
    }
}
