package io.deephaven.engine.updategraph.impl;

import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import org.jetbrains.annotations.NotNull;

final class PoisonedNotificationProcessor implements AbstractUpdateGraph.NotificationProcessor {

    static final AbstractUpdateGraph.NotificationProcessor INSTANCE = new PoisonedNotificationProcessor();

    private static RuntimeException notYetStarted() {
        return new IllegalStateException("PeriodicUpdateGraph has not been started yet");
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
