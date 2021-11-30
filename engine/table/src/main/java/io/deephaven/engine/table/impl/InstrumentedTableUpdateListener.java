/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import org.jetbrains.annotations.Nullable;

public abstract class InstrumentedTableUpdateListener extends InstrumentedTableListenerBase
        implements TableUpdateListener {

    public InstrumentedTableUpdateListener(@Nullable final String description) {
        super(description, false);
    }

    public InstrumentedTableUpdateListener(@Nullable final String description, final boolean terminalListener) {
        super(description, terminalListener);
    }

    @Override
    public Notification getNotification(final TableUpdate update) {
        return new Notification(update);
    }

    /**
     * Delivers the desired update, bracketed by performance instrumentation.
     */
    public class Notification extends NotificationBase {

        Notification(final TableUpdate update) {
            super(update);
        }

        @Override
        public void run() {
            doRun(() -> onUpdate(update));
        }
    }
}
