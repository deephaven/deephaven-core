package io.deephaven.engine.tables.live;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * On run, each of the enqueued runnables is executed.
 */
public class OneShotRefreshCombiner implements LiveTable, UpdateRootRegistrar {
    private final Deque<Runnable> runnables = new ArrayDeque<>();

    @Override
    public void run() {
        while (true) {
            final Runnable oneshot;
            synchronized (runnables) {
                oneshot = runnables.pollFirst();
            }
            if (oneshot == null) {
                return;
            }
            oneshot.run();
        }
    }

    @Override
    public void addTable(@NotNull Runnable updateRoot) {
        synchronized (runnables) {
            runnables.add(updateRoot);
        }
    }

    @Override
    public void removeTable(@NotNull Runnable runnable) {
        synchronized (runnables) {
            runnables.remove(runnable);
        }
    }

    @Override
    public void requestRefresh(@NotNull Runnable table) {
        UpdateGraphProcessor.DEFAULT.requestRefresh(table);
    }

    @Override
    public void maybeRefreshTable(@NotNull Runnable table, boolean onlyIfHaveLock) {
        UpdateGraphProcessor.DEFAULT.maybeRefreshTable(table, onlyIfHaveLock);
    }
}
