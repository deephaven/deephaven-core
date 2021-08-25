/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

import java.util.concurrent.TimeUnit;

/**
 * A multithreaded resource to execute data driven models.
 */
public interface ModelFarm {

    /**
     * Initiates execution.
     */
    void start();

    /**
     * Initiates an orderly shutdown in which previously submitted tasks are executed, but no new tasks will be
     * accepted. Invocation has no additional effect if already shut down.
     * <p>
     * <p>
     * This method does not wait for previously submitted tasks to complete execution. Use {@link #awaitTermination
     * awaitTermination} to do that.
     */
    void shutdown();

    /**
     * Initiates an immediate termination of all tasks. Unexecuted tasks will not be executed. Tasks already executing
     * may not be interrupted.
     */
    void terminate();

    /**
     * Blocks until all tasks have completed execution after a shutdown request.
     *
     * @return {@code true} if this executor terminated and {@code false} if the timeout elapsed before termination
     */
    boolean awaitTermination();

    /**
     * Blocks until all tasks have completed execution after a shutdown request, or the timeout occurs, whichever
     * happens first.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return {@code true} if this executor terminated and {@code false} if the timeout elapsed before termination
     */
    boolean awaitTermination(final long timeout, final TimeUnit unit);

    /**
     * Shuts down and then awaits termination.
     */
    void shutdownAndAwaitTermination();

    /**
     * Shuts down and then awaits termination.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return {@code true} if this executor terminated and {@code false} if the timeout elapsed before termination
     */
    boolean shutdownAndAwaitTermination(final long timeout, final TimeUnit unit);
}
