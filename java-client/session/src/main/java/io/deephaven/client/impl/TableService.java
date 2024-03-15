//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.qst.TableCreationLogic;
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
     * <p>
     * When {@code mixinStacktraces == true}, preemptive stacktraces will taken in the the {@link TableCreationLogic}
     * methods. While relatively expensive, in exceptional circumstances this mixin allows errors to be more
     * appropriately attributed with their source.
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
         * Waits if necessary for the computation to complete, and then retrieves its result. If the current thread is
         * interrupted while waiting, {@link #cancel(boolean)} will be invoked.
         *
         * <p>
         * After this method returns (normally or exceptionally), {@code this} future will be {@link #isDone() done}.
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
         * its result. If the current thread is interrupted while waiting or times out, {@link #cancel(boolean)} will be
         * invoked.
         *
         * <p>
         * After this method returns (normally or exceptionally), {@code this} future will be {@link #isDone() done}.
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
            FutureHelper.cancelOrConsume(futures, (tableHandle, e, c) -> {
                if (tableHandle != null) {
                    tableHandle.close();
                }
            }, mayInterruptIfRunning);
        }
    }
}
