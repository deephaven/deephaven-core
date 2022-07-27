/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.updategraph;

import io.deephaven.engine.context.ExecutionContext;

public abstract class TerminalNotification extends AbstractNotification {

    protected TerminalNotification() {
        super(true);
    }

    @Override
    public boolean canExecute(final long step) {
        throw new UnsupportedOperationException("Terminal notifications do not have dependency information.");
    }

    @Override
    public ExecutionContext getExecutionContext() {
        return null;
    }
}
