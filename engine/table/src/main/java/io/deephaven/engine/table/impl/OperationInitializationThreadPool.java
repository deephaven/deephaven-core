/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.thread.NamingThreadFactory;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class OperationInitializationThreadPool {

    /**
     * The number of threads that will be used for parallel initialization in this process
     */
    public static final int NUM_THREADS;

    static {
        final int numThreads =
                Configuration.getInstance().getIntegerWithDefault("OperationInitializationThreadPool.threads", -1);
        if (numThreads <= 0) {
            NUM_THREADS = Runtime.getRuntime().availableProcessors();
        } else {
            NUM_THREADS = numThreads;
        }
    }

    private static final ThreadLocal<Boolean> isInitializationThread = ThreadLocal.withInitial(() -> false);

    /**
     * @return Whether the current thread is part of the OperationInitializationThreadPool's {@link #executorService()}
     */
    public static boolean isInitializationThread() {
        return isInitializationThread.get();
    }

    /**
     * @return Whether the current thread can parallelize operations using the OperationInitializationThreadPool's
     *         {@link #executorService()}
     */
    public static boolean canParallelize() {
        return NUM_THREADS > 1 && !isInitializationThread();
    }

    private static final ThreadPoolExecutor executorService;

    static {
        final ThreadGroup threadGroup = new ThreadGroup("OperationInitializationThreadPool");
        final ThreadFactory threadFactory = new NamingThreadFactory(
                threadGroup, OperationInitializationThreadPool.class, "initializationExecutor", true) {
            @Override
            public Thread newThread(@NotNull final Runnable r) {
                return super.newThread(ThreadInitializationFactory.wrapRunnable(() -> {
                    isInitializationThread.set(true);
                    MultiChunkPool.enableDedicatedPoolForThisThread();
                    r.run();
                }));
            }
        };
        executorService = new ThreadPoolExecutor(
                NUM_THREADS, NUM_THREADS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);
    }

    /**
     * @return The OperationInitializationThreadPool's {@link ExecutorService}; will be {@code null} if the
     *         OperationInitializationThreadPool has not been {@link #start() started}
     */
    public static ExecutorService executorService() {
        return executorService;
    }

    /**
     * Start the OperationInitializationThreadPool. In practice, this just pre-starts all threads in the
     * {@link #executorService()}.
     */
    public static void start() {
        executorService.prestartAllCoreThreads();
    }
}
