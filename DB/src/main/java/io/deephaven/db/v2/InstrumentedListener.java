/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.v2.utils.AbstractIndexUpdateNotification;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import org.jetbrains.annotations.Nullable;

public abstract class InstrumentedListener extends InstrumentedListenerBase implements Listener {

    private Index initialImage;
    private Index initialImageClone;

    public InstrumentedListener(@Nullable final String description) {
        super(description, false);
    }

    public InstrumentedListener(@Nullable final String description,
        final boolean terminalListener) {
        super(description, terminalListener);
    }

    @Override
    public AbstractIndexUpdateNotification getNotification(final Index added, final Index removed,
        final Index modified) {
        return new Notification(added, removed, modified);
    }

    /**
     * Delivers the desired update, bracketed by performance instrumentation.
     */
    public class Notification extends NotificationBase {

        Notification(final Index added, final Index removed, final Index modified) {
            super(new ShiftAwareListener.Update(added.clone(), removed.clone(), modified.clone(),
                IndexShiftData.EMPTY, ModifiedColumnSet.ALL));
            update.release(); // NotificationBase assumes it does not own the provided update.
        }

        @Override
        public void run() {
            doRun(() -> {
                if (initialImage != null
                    && (initialImage != update.added || update.removed.nonempty()
                        || update.modified.nonempty())) {
                    onUpdate(update.added.minus(initialImageClone), update.removed,
                        update.modified);
                } else {
                    onUpdate(update.added, update.removed, update.modified);
                }
                initialImage = initialImageClone = null;
            });
        }
    }

    @Override
    public void setInitialImage(final Index initialImage) {
        this.initialImage = initialImage;
        this.initialImageClone = initialImage.clone();
    }
}
