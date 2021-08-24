/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.v2.utils.AbstractIndexUpdateNotification;
import org.jetbrains.annotations.Nullable;

public abstract class InstrumentedShiftAwareListener extends InstrumentedListenerBase
    implements ShiftAwareListener {

    public InstrumentedShiftAwareListener(@Nullable final String description) {
        super(description, false);
    }

    public InstrumentedShiftAwareListener(@Nullable final String description,
        final boolean terminalListener) {
        super(description, terminalListener);
    }

    @Override
    public AbstractIndexUpdateNotification getNotification(final Update update) {
        return new Notification(update);
    }

    /**
     * Delivers the desired update, bracketed by performance instrumentation.
     */
    public class Notification extends NotificationBase {

        Notification(final Update update) {
            super(update);
        }

        @Override
        public void run() {
            doRun(() -> onUpdate(update));
        }
    }
}

