/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.qst.table.TableSpec;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface TableServiceAsync {

    interface TableHandleFuture extends Future<TableHandle> {

        /**
         * Waits if necessary for the computation to complete, and then retrieves its result. If an
         * {@link InterruptedException} is thrown, the future will be {@link #cancelOrClose(TableHandleFuture, boolean)
         * cancelled or closed}.
         *
         * @param future the future
         * @return the table handle
         * @throws InterruptedException if the current thread was interrupted while waiting
         * @throws ExecutionException if the computation threw an exception
         * @throws CancellationException if the computation was cancelled
         */
        static TableHandle get(TableHandleFuture future) throws InterruptedException, ExecutionException {
            try {
                return future.get();
            } catch (InterruptedException e) {
                cancelOrClose(future, true);
                throw e;
            }
        }

        /**
         * Waits if necessary for at most the given {@code timeout} for the computation to complete, and then retrieves
         * its result, if available. If an {@link InterruptedException} or {@link TimeoutException} is thrown, the
         * future will be {@link #cancelOrClose(TableHandleFuture, boolean) cancelled or closed}.
         *
         * @param timeout the maximum time to wait
         * @return the table handle
         * @throws CancellationException if the computation was cancelled
         * @throws ExecutionException if the computation threw an exception
         * @throws InterruptedException if the current thread was interrupted while waiting
         * @throws TimeoutException if the wait timed out
         */
        static TableHandle get(TableHandleFuture future, Duration timeout)
                throws InterruptedException, ExecutionException, TimeoutException {
            try {
                return future.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            } catch (InterruptedException | TimeoutException e) {
                cancelOrClose(future, true);
                throw e;
            }
        }

        /**
         * Cancels {@code future} or closes the {@link TableHandle} if {@code future} completed normally. If
         * {@code future} has completed exceptionally, the {@link ExecutionException} will be disregarded. If {@code
         * future} has been cancelled, the {@link CancellationException} will be disregarded.
         *
         * @param future the future
         * @param mayInterruptIfRunning {@code true} if the thread executing this task should be interrupted; otherwise,
         *        in-progress tasks are allowed to complete
         */
        static void cancelOrClose(TableHandleFuture future, boolean mayInterruptIfRunning) {
            if (future.cancel(mayInterruptIfRunning)) {
                // Cancel was successful
                return;
            }
            // Cancel was not successful, the future is already done
            boolean interrupted = false;
            try {
                while (true) {
                    try (final TableHandle ignored = future.get(0, TimeUnit.SECONDS)) {
                        // On completed normally, close the handle
                        return;
                    } catch (ExecutionException | CancellationException e) {
                        // On completed exceptionally or cancelled, nothing to do
                        return;
                    } catch (InterruptedException e) {
                        // On interrupted, ensure interrupt status propagates
                        interrupted = true;
                    } catch (TimeoutException e) {
                        // On timeout, not expected
                        throw new IllegalStateException(e);
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        /**
         * Cancels all of the {@code futures} first and then closes any {@link TableHandle} from {@code futures} that
         * completed normally. Any {@link ExecutionException ExecptionExceptions} will be disregarded. If {@code future}
         * has been cancelled, the {@link CancellationException} will be disregarded.
         *
         * @param futures the futures
         * @param mayInterruptIfRunning {@code true} if the thread executing the task should be interrupted; otherwise,
         *        in-progress tasks are allowed to complete
         */
        static void cancelOrClose(Iterable<? extends TableHandleFuture> futures, boolean mayInterruptIfRunning) {
            for (TableHandleFuture future : futures) {
                future.cancel(mayInterruptIfRunning);
            }
            for (TableHandleFuture future : futures) {
                cancelOrClose(future, mayInterruptIfRunning);
            }
        }
    }

    /**
     * Executes the given {@code table} and returns a future. If applicable, the request will build off of the existing
     * exports.
     *
     * @param table the table spec
     * @return the table handle future
     */
    TableHandleFuture executeAsync(TableSpec table);

    /**
     * Executes the given {@code tables} and returns a future for each. If applicable, the request will build off of the
     * existing exports.
     *
     * @param tables the tables specs
     * @return the table handle futures
     */
    List<? extends TableHandleFuture> executeAsync(Iterable<? extends TableSpec> tables);
}
