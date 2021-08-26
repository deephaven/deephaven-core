/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.process;

import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public interface ShutdownManager {

    /**
     * Categories that define ordering "groups" for serial execution during task invocation.
     */
    enum OrderingCategory {

        /**
         * Tasks that should be kicked off before the rest. For example, disconnecting clients that
         * may otherwise be poorly served during shutdown processing, or may cause shutdown delays
         * by adding additional work.
         */
        FIRST,

        /**
         * Tasks that have no particular timeliness requirement. For example, flushing persistent
         * stores to permanent storage.
         */
        MIDDLE,

        /**
         * Tasks that should be dispatched after others. For example, shutting down a logger
         * framework and flushing log data.
         */
        LAST
    }

    /**
     * A single unit of work for shutdown.
     */
    @FunctionalInterface
    interface Task {

        /**
         * Invoke this task for shutdown processing.
         */
        void invoke();
    }

    /**
     * Add a shutdown hook to the runtime that will invoke all registered shutdown tasks, if they
     * haven't previously been invoked.
     */
    void addShutdownHookToRuntime();

    /**
     * Register task for shutdown invocation along with other tasks belonging to orderingCategory.
     * Registration concurrent with invocation (that is, shutdown in progress) is not guaranteed to
     * be effective.
     *
     * @param orderingCategory
     * @param task
     */
    void registerTask(@NotNull OrderingCategory orderingCategory, @NotNull Task task);

    /**
     * Remove the most recent registration of task with orderingCategory. De-registration concurrent
     * with invocation (that is, shutdown in progress) is not guaranteed to be effective.
     *
     * @param orderingCategory
     * @param task
     */
    void deregisterTask(@NotNull OrderingCategory orderingCategory, @NotNull Task task);

    /**
     * Clear all shutdown tasks and reset internal state. Useful for unit tests, not safe for
     * production use cases.
     */
    void reset();

    /**
     * @return True if shutdown tasks have been invoked (meaning shutdown is in progress).
     */
    boolean tasksInvoked();

    /**
     * Invoke all registered shutdown tasks, if they haven't previously been invoked.
     *
     * @return True if shutdown task invocation was performed by this call
     */
    boolean maybeInvokeTasks();
}
