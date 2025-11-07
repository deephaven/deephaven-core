//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

public abstract class InstrumentedTableUpdateListener extends InstrumentedTableListenerBase
        implements TableUpdateListener {

    public InstrumentedTableUpdateListener(@Nullable final String description) {
        super(description, false, null);
    }

    public InstrumentedTableUpdateListener(@Nullable final String description, final boolean terminalListener) {
        super(description, terminalListener, null);
    }

    public InstrumentedTableUpdateListener(@Nullable final String description, final boolean terminalListener,
            @Nullable final Supplier<long[]> ancestors) {
        super(description, terminalListener, ancestors);
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
