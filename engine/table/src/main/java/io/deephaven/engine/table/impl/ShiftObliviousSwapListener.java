package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ShiftObliviousListener;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.rowset.RowSet;

public class ShiftObliviousSwapListener extends SwapListenerBase<ShiftObliviousListener>
        implements ShiftObliviousListener {

    public ShiftObliviousSwapListener(BaseTable sourceTable) {
        super(sourceTable);
    }

    @Override
    public synchronized void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
        // not a direct listener
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized NotificationQueue.Notification getNotification(
            final RowSet added, final RowSet removed, final RowSet modified) {
        return doGetNotification(() -> eventualListener.getNotification(added, removed, modified));
    }

    @Override
    public void setInitialImage(RowSet initialImage) {
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
