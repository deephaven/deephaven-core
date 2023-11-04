/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

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

    static <T> void cancelOrConsume(Iterable<? extends Future<T>> futures, BiConsumer<T, ExecutionException> consumer,
            boolean mayInterruptIfRunning) {
        for (Future<T> future : futures) {
            if (future.cancel(mayInterruptIfRunning)) {
                continue;
            }
            final T normal;
            try {
                normal = getCompleted(future, false);
            } catch (ExecutionException e) {
                consumer.accept(null, e);
                continue;
            } catch (CancellationException e) {
                continue;
            }
            consumer.accept(normal, null);
        }
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
