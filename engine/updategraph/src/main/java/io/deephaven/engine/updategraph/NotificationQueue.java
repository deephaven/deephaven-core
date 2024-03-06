//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.exceptions.UpdateGraphConflictException;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

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
         * If a terminal notification must be executed serially (typically under an UpdateGraph's exclusive lock), it
         * must override this method so that the notification is not executed concurrently with other notifications.
         * <p>
         * It is an error to return true if this notification is not terminal
         *
         * @return true if this notification must be executed serially
         */
        boolean mustExecuteWithUpdateGraphLock();

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
         * @throws ClockInconsistencyException if step is observed to be before the highest step known to this
         *         Dependency; this is a best effort validation, in order to allow concurrent snapshots to fail fast or
         *         improper update processing to be detected
         * @implNote For all practical purposes, all implementations should consider whether the {@link UpdateGraph}
         *           itself is satisfied if they have no other dependencies.
         */
        boolean satisfied(long step);

        /**
         * @return the update graph that this dependency is a part of
         */
        UpdateGraph getUpdateGraph();

        default UpdateGraph getUpdateGraph(Dependency... dependencies) {
            return NotificationQueue.Dependency.getUpdateGraph(this, dependencies);
        }

        /**
         * Examine all {@code dependencies} excluding non-refreshing {@link DynamicNode dynamic nodes}, and verify that
         * they are using the same {@link UpdateGraph}.
         * <p>
         * If a singular update graph was found in this process, return it.
         * <p>
         * Otherwise, if all dependencies are non-refreshing {@link DynamicNode dynamic nodes}, return null.
         *
         * @param first at least one dependency is helpful
         * @param dependencies the dependencies to examine
         * @return the singular {@link UpdateGraph} used by all {@code dependencies}, or null if all
         *         {@code dependencies} are non-refreshing {@link DynamicNode dynamic nodes}
         * @throws UpdateGraphConflictException if multiple update graphs were found in the dependencies
         */
        static UpdateGraph getUpdateGraph(@Nullable Dependency first, Dependency... dependencies) {
            UpdateGraph graph = null;
            UpdateGraph firstNonNullGraph = null;

            if (first != null) {
                firstNonNullGraph = first.getUpdateGraph();
                if (!DynamicNode.isDynamicAndNotRefreshing(first)) {
                    graph = first.getUpdateGraph();
                }
            }

            for (final Dependency other : dependencies) {
                if (other != null && firstNonNullGraph == null) {
                    firstNonNullGraph = other.getUpdateGraph();
                }
                if (other == null || DynamicNode.isDynamicAndNotRefreshing(other)) {
                    continue;
                }
                if (graph == null) {
                    graph = other.getUpdateGraph();
                } else if (graph != other.getUpdateGraph()) {
                    throw new UpdateGraphConflictException("Multiple update graphs found in dependencies: " + graph
                            + " and " + other.getUpdateGraph());
                }
            }

            return graph == null ? firstNonNullGraph : graph;
        }
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
     * Enqueue a collection of notifications to be flushed.
     *
     * @param notifications The notification to enqueue
     * @see #addNotification(Notification)
     */
    void addNotifications(@NotNull final Collection<? extends Notification> notifications);

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
