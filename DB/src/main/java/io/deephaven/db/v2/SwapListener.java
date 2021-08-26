package io.deephaven.db.v2;

import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.v2.utils.Index;

public class SwapListener extends SwapListenerBase<Listener> implements Listener {

    public SwapListener(BaseTable sourceTable) {
        super(sourceTable);
    }

    @Override
    public synchronized void onUpdate(final Index added, final Index removed, final Index modified) {
        // not a direct listener
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized NotificationQueue.IndexUpdateNotification getNotification(
            final Index added, final Index removed, final Index modified) {
        return doGetNotification(() -> eventualListener.getNotification(added, removed, modified));
    }

    @Override
    public void setInitialImage(Index initialImage) {
        // we should never use an initialImage, because the swapListener listens to the table before we are confident
        // that we'll get a good snapshot, and if we get a bad snapshot, it will never get updated appropriately
        throw new IllegalStateException();
    }

    @Override
    public void destroy() {
        super.destroy();
        sourceTable.removeUpdateListener(this);
        sourceTable.removeDirectUpdateListener(this);
    }

    @Override
    public void subscribeForUpdates() {
        sourceTable.listenForUpdates(this);
    }
}
