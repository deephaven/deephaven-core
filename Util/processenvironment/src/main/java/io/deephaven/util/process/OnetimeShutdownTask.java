//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.process;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class OnetimeShutdownTask implements ShutdownManager.Task {

    /**
     * NB: This doesn't need to be an AtomicBoolean, only a volatile boolean, but we use the object for its monitor.
     */
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    public final boolean isShutdown() {
        return isShutdown.get();
    }

    @Override
    public final void invoke() {
        if (isShutdown()) {
            return;
        }
        synchronized (isShutdown) {
            if (isShutdown()) {
                return;
            }
            isShutdown.set(true);
            shutdown();
            isShutdown.notifyAll();
        }
    }

    public final void awaitShutdown() {
        synchronized (isShutdown) {
            while (!isShutdown()) {
                try {
                    isShutdown.wait();
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    public final void awaitShutdown(final long waitMillis, final Runnable task) {
        synchronized (isShutdown) {
            while (!isShutdown()) {
                try {
                    isShutdown.wait(waitMillis);
                } catch (InterruptedException ignored) {
                }
                if (!isShutdown()) {
                    task.run();
                }
            }
        }
    }

    protected abstract void shutdown();

    public static OnetimeShutdownTask adapt(@NotNull final Runnable shutdown) {
        return new OnetimeShutdownTask() {
            @Override
            protected void shutdown() {
                shutdown.run();
            }
        };
    }
}
