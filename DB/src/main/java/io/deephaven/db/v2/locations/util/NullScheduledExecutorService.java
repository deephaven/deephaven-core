/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations.util;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * This is a dummy ScheduledExecutorService that we use in TableDataRefreshService when we don't actually want to execute
 * any background refresh tasks.  It only implements the methods needed for that use case, and is intended for unit
 * tests.
 */
@SuppressWarnings("unused")
public class NullScheduledExecutorService implements ScheduledExecutorService {

    private static class NullScheduledFuture<V> implements ScheduledFuture<V> {

        @Override
        public long getDelay(@NotNull TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(@NotNull Delayed o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public V get() {
            throw new UnsupportedOperationException();
        }

        @Override
        public V get(long timeout, @NotNull TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
    }

    @NotNull
    @Override
    public ScheduledFuture<?> schedule(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <V> ScheduledFuture<V> schedule(@NotNull Callable<V> callable, long delay, @NotNull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command, long initialDelay, long period, @NotNull TimeUnit unit) {
        return new NullScheduledFuture();
    }

    @NotNull
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(@NotNull Runnable command, long initialDelay, long delay, @NotNull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public List<Runnable> shutdownNow() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isShutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isTerminated() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> Future<T> submit(@NotNull Callable<T> task) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> Future<T> submit(@NotNull Runnable task, T result) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Future<?> submit(@NotNull Runnable task) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks, long timeout, @NotNull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks, long timeout, @NotNull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void execute(@NotNull Runnable command) {
        throw new UnsupportedOperationException();
    }
}
