package io.deephaven.engine.updategraph.impl;

import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import org.jetbrains.annotations.NotNull;

/**
 * The poisoned notification processor is used when an update graph has not yet been started, throwing an
 * IllegalStateException on all operations.
 */
final class PoisonedNotificationProcessor implements BaseUpdateGraph.NotificationProcessor {

    static final BaseUpdateGraph.NotificationProcessor INSTANCE = new PoisonedNotificationProcessor();

    private static RuntimeException notYetStarted() {
        return new IllegalStateException("UpdateGraph has not been started yet");
    }

    private PoisonedNotificationProcessor() {}

    @Override
    public void submit(@NotNull NotificationQueue.Notification notification) {
        throw notYetStarted();
    }

    @Override
    public void submitAll(@NotNull IntrusiveDoublyLinkedQueue<NotificationQueue.Notification> notifications) {
        throw notYetStarted();
    }

    @Override
    public int outstandingNotificationsCount() {
        throw notYetStarted();
    }

    @Override
    public void doWork() {
        throw notYetStarted();
    }

    @Override
    public void doAllWork() {
        throw notYetStarted();
    }

    @Override
    public void shutdown() {}

    @Override
    public void onNotificationAdded() {
        throw notYetStarted();
    }

    @Override
    public void beforeNotificationsDrained() {
        throw notYetStarted();
    }
}
