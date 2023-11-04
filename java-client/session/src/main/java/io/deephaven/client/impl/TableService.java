/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.qst.table.TableSpec;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public interface TableService extends TableHandleManager {

    /**
     * A batch table handle manager.
     *
     * @return a batch manager
     */
    TableHandleManager batch();

    /**
     * A batch table handle manager.
     *
     * @param mixinStacktraces if stacktraces should be mixin
     * @return a batch manager
     */
    TableHandleManager batch(boolean mixinStacktraces);

    /**
     * A serial table handle manager.
     *
     * @return a serial manager
     */
    TableHandleManager serial();

    /**
     * Executes the given {@code table} and returns a future. If this is a stateful instance, the request will build off
     * of the existing exports.
     *
     * @param table the table spec
     * @return the table handle future
     */
    TableHandleFuture executeAsync(TableSpec table);

    /**
     * Executes the given {@code tables} and returns a future for each. If this is a stateful instance, the request will
     * build off of the existing exports.
     *
     * @param tables the tables specs
     * @return the table handle futures
     */
    List<? extends TableHandleFuture> executeAsync(Iterable<? extends TableSpec> tables);

    interface TableHandleFuture extends Future<TableHandle> {

        /**
         * Waits if necessary for the computation to complete, and then retrieves its result. If an
         * {@link InterruptedException} is thrown, the future will be cancelled. If the cancellation is successful, the
         * the exception will be re-thrown; otherwise, the normally completed value will be returned, or the
         * {@link ExecutionException} will be thrown, with {@link Thread#interrupt()} invoked on the current thread.
         *
         * @return the table handle
         * @throws InterruptedException if the current thread was interrupted while waiting
         * @throws ExecutionException if the computation threw an exception
         * @throws CancellationException if the computation was cancelled while waiting
         */
        default TableHandle getOrCancel() throws InterruptedException, ExecutionException {
            return FutureHelper.getOrCancel(this);
        }

        /**
         * Waits if necessary for at most the given {@code timeout} for the computation to complete, and then retrieves
         * its result. If an {@link InterruptedException} or {@link TimeoutException} is thrown, the future will be
         * cancelled. If the cancellation is successful, the exception will be re-thrown; otherwise, the normally
         * completed value will be returned, or the {@link ExecutionException} will be thrown, with
         * {@link Thread#interrupt()} invoked on the current thread if an {@link InterruptedException} has been thrown
         * during this process.
         *
         * @return the table handle
         * @throws InterruptedException if the current thread was interrupted while waiting
         * @throws ExecutionException if the computation threw an exception
         * @throws CancellationException if the computation was cancelled while waiting
         */
        default TableHandle getOrCancel(Duration timeout)
                throws InterruptedException, ExecutionException, TimeoutException {
            return FutureHelper.getOrCancel(this, timeout);
        }

        /**
         * Cancels all of the {@code futures} and closes any {@link TableHandle} from {@code futures} that completed
         * normally. Any {@link ExecutionException ExecptionExceptions} or {@link CancellationException
         * CancellationException} will be disregarded.
         *
         * @param futures the futures
         * @param mayInterruptIfRunning {@code true} if the thread executing the task should be interrupted; otherwise,
         *        in-progress tasks are allowed to complete
         */
        static void cancelOrClose(Iterable<? extends TableHandleFuture> futures, boolean mayInterruptIfRunning) {
            FutureHelper.cancelOrConsume(futures, (tableHandle, e) -> {
                if (tableHandle != null) {
                    tableHandle.close();
                }
            }, mayInterruptIfRunning);
        }
    }
}
