/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.updategraph;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for notification of update graph node changes.
 */
public interface NotificationQueue {

    /**
     * A notification that may be enqueued.
     */
    interface Notification extends Runnable, LogOutputAppendable, IntrusiveDoublyLinkedNode<Notification> {
        /**
         * Terminal notifications guarantee that they will not queue additional notifications or mutate data structures
         * that should result in additional notifications. They are in turn guaranteed to be called after all
         * non-terminal notifications for a given cycle through the notification queue.
         * 
         * @return True iff this notification is terminal.
         */
        boolean isTerminal();

        /**
         * If a terminal notification must be executed on the main UGP thread, it must override this method, so that the
         * notification is not executed on the run pool.
         *
         * It is an error to return true if this notification is not terminal
         *
         * @return true if this notification must be executed directly under the protection of the UGP lock
         */
        boolean mustExecuteWithUgpLock();

        /**
         * Can this notification be executed? That is, are all of it's dependencies satisfied.
         *
         * @param step The step for which we are testing satisfaction
         * @return true if this notification can be executed, false if it has unmet dependencies
         */
        boolean canExecute(long step);
    }

    /**
     * Marker interface for error notifications, only relevant to unit testing framework.
     */
    interface ErrorNotification extends Notification {
    }

    interface Dependency extends LogOutputAppendable {
        /**
         * Is this ancestor satisfied? Note that this method must be safe to call on any thread.
         *
         * @param step The step for which we are testing satisfaction
         * @return Whether the dependency is satisfied on {@code step} (and will not fire subsequent notifications)
         * @implNote For all practical purposes, all implementations should consider whether the
         *           {@link UpdateGraphProcessor} itself is satisfied if they have no other dependencies.
         */
        boolean satisfied(long step);
    }

    /**
     * Add a notification for this NotificationQueue to deliver (by invoking its run() method). Note that
     * implementations may have restrictions as to how and when this method may be used for non-terminal notifications,
     * e.g. by only supporting notification queuing from threads that can guarantee they are part of an update cycle.
     *
     * @param notification The notification to add
     */
    void addNotification(@NotNull Notification notification);

    /**
     * Add a notification for this NotificationQueue to deliver (by invoking its run() method), iff the delivery step is
     * the current step and the update cycle for that step is still in process. This is only supported for non-terminal
     * notifications.
     *
     * @param notification The notification to add
     * @param deliveryStep The step to deliver this notification on
     */
    boolean maybeAddNotification(@NotNull Notification notification, long deliveryStep);
}
