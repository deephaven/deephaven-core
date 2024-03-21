//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

public abstract class TerminalNotification extends AbstractNotification {

    protected TerminalNotification() {
        super(true);
    }

    @Override
    public boolean canExecute(final long step) {
        throw new UnsupportedOperationException("Terminal notifications do not have dependency information.");
    }
}
