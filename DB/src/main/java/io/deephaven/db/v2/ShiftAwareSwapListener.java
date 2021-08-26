package io.deephaven.db.v2;

import io.deephaven.db.tables.live.NotificationQueue;

public class ShiftAwareSwapListener extends SwapListenerBase<ShiftAwareListener>
    implements ShiftAwareListener {

    public ShiftAwareSwapListener(final BaseTable sourceTable) {
        super(sourceTable);
    }

    @Override
    public synchronized void onUpdate(final Update upstream) {
        // not a direct listener
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized NotificationQueue.IndexUpdateNotification getNotification(
        final Update update) {
        return doGetNotification(() -> eventualListener.getNotification(update));
    }

    @Override
    public void destroy() {
        super.destroy();
        sourceTable.removeUpdateListener(this);
    }

    @Override
    public void subscribeForUpdates() {
        sourceTable.listenForUpdates(this);
    }
}
