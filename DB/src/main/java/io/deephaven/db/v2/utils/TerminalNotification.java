package io.deephaven.db.v2.utils;

public abstract class TerminalNotification extends AbstractNotification {
    protected TerminalNotification() {
        super(true);
    }

    @Override
    public boolean canExecute(final long step) {
        throw new UnsupportedOperationException(
            "Terminal notifications do not have dependency information.");
    }
}
