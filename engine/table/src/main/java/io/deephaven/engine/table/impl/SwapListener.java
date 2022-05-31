package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.updategraph.NotificationQueue;

public class SwapListener extends SwapListenerBase<TableUpdateListener> implements TableUpdateListener {

    public SwapListener(final BaseTable sourceTable) {
        super(sourceTable);
    }

    @Override
    public synchronized void onUpdate(final TableUpdate upstream) {
        // not a direct listener
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized NotificationQueue.Notification getNotification(final TableUpdate update) {
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
