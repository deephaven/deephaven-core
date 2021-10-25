/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2;

import io.deephaven.engine.v2.utils.AbstractIndexUpdateNotification;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.RowSetShiftData;
import org.jetbrains.annotations.Nullable;

public abstract class ShiftObliviousInstrumentedListener extends InstrumentedListenerBase implements ShiftObliviousListener {

    private RowSet initialImage;
    private RowSet initialImageClone;

    public ShiftObliviousInstrumentedListener(@Nullable final String description) {
        super(description, false);
    }

    public ShiftObliviousInstrumentedListener(@Nullable final String description, final boolean terminalListener) {
        super(description, terminalListener);
    }

    @Override
    public AbstractIndexUpdateNotification getNotification(final RowSet added, final RowSet removed,
            final RowSet modified) {
        return new Notification(added, removed, modified);
    }

    /**
     * Delivers the desired update, bracketed by performance instrumentation.
     */
    public class Notification extends NotificationBase {

        Notification(final RowSet added, final RowSet removed, final RowSet modified) {
            super(new Listener.Update(added.clone(), removed.clone(), modified.clone(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.ALL));
            update.release(); // NotificationBase assumes it does not own the provided update.
        }

        @Override
        public void run() {
            doRun(() -> {
                if (initialImage != null && (initialImage != update.added || update.removed.isNonempty()
                        || update.modified.isNonempty())) {
                    onUpdate(update.added.minus(initialImageClone), update.removed, update.modified);
                } else {
                    onUpdate(update.added, update.removed, update.modified);
                }
                initialImage = initialImageClone = null;
            });
        }
    }

    @Override
    public void setInitialImage(final RowSet initialImage) {
        this.initialImage = initialImage;
        this.initialImageClone = initialImage.clone();
    }
}
