package io.deephaven.db.tables.live;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * On refresh, each of the enqueued runnables is executed.
 */
public class OneShotRefreshCombiner implements LiveTable, LiveTableRegistrar {
    private final Deque<LiveTable> runnables = new ArrayDeque<>();

    @Override
    public void refresh() {
        while (true) {
            final LiveTable oneshot;
            synchronized (runnables) {
                oneshot = runnables.pollFirst();
            }
            if (oneshot == null) {
                return;
            }
            oneshot.refresh();
        }
    }

    @Override
    public void addTable(@NotNull LiveTable liveTable) {
        synchronized (runnables) {
            runnables.add(liveTable);
        }
    }

    @Override
    public void removeTable(@NotNull LiveTable runnable) {
        synchronized (runnables) {
            runnables.remove(runnable);
        }
    }

    @Override
    public void requestRefresh(@NotNull LiveTable table) {
        LiveTableMonitor.DEFAULT.requestRefresh(table);
    }

    @Override
    public void maybeRefreshTable(@NotNull LiveTable table, boolean onlyIfHaveLock) {
        LiveTableMonitor.DEFAULT.maybeRefreshTable(table, onlyIfHaveLock);
    }
}
