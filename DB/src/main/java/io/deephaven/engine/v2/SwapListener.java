package io.deephaven.engine.v2;

import io.deephaven.engine.updategraph.NotificationQueue;

public class SwapListener extends SwapListenerBase<Listener> implements Listener {

    public SwapListener(final BaseTable sourceTable) {
        super(sourceTable);
    }

    @Override
    public synchronized void onUpdate(final Update upstream) {
        // not a direct listener
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized NotificationQueue.Notification getNotification(final Update update) {
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
