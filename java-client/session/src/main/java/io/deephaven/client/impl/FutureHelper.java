//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class FutureHelper {


    static <T> T getOrCancel(Future<T> future) throws InterruptedException, ExecutionException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            if (future.cancel(true)) {
                throw e;
            }
            return getCompleted(future, true);
        }
    }

    static <T> T getOrCancel(Future<T> future, Duration timeout)
            throws InterruptedException, ExecutionException, TimeoutException {
        try {
            return future.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            if (future.cancel(true)) {
                throw e;
            }
            return getCompleted(future, true);
        } catch (TimeoutException e) {
            if (future.cancel(true)) {
                throw e;
            }
            return getCompleted(future, false);
        }
    }

    @FunctionalInterface
    interface FutureConsumer<T> {
        void accept(T result, ExecutionException e, CancellationException c);
    }

    /**
     * Invokes {@link Future#cancel(boolean)} for each future in {@code futures}. If the cancel is not successful, the
     * completed result will be passed to {@code consumer}.
     *
     * @param futures the futures
     * @param consumer the consumer
     * @param mayInterruptIfRunning {@code true} if the thread executing the task should be interrupted; otherwise,
     *        in-progress tasks are allowed to complete
     * @param <T> The result type for the {@code futures}
     */
    static <T> void cancelOrConsume(
            Iterable<? extends Future<T>> futures, FutureConsumer<T> consumer, boolean mayInterruptIfRunning) {
        for (Future<T> future : futures) {
            if (future.cancel(mayInterruptIfRunning)) {
                continue;
            }
            consumeCompleted(future, consumer);
        }
    }

    private static <T> void consumeCompleted(Future<T> future, FutureConsumer<T> consumer) {
        final T result;
        try {
            result = getCompleted(future, false);
        } catch (ExecutionException e) {
            consumer.accept(null, e, null);
            return;
        } catch (CancellationException c) {
            consumer.accept(null, null, c);
            return;
        }
        consumer.accept(result, null, null);
    }


    private static <T> T getCompleted(Future<T> future, boolean interrupted) throws ExecutionException {
        // We know future is done
        try {
            while (true) {
                try {
                    return future.get();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
