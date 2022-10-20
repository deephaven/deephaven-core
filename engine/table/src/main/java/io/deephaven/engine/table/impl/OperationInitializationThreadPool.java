/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.thread.NamingThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OperationInitializationThreadPool {

    public static final int NUM_THREADS;

    static {
        final int numThreads =
                Configuration.getInstance().getIntegerWithDefault("OperationInitializationThreadPool.threads", 1);
        if (numThreads <= 0) {
            NUM_THREADS = Runtime.getRuntime().availableProcessors();
        } else {
            NUM_THREADS = numThreads;
        }
    }

    private static final ThreadLocal<Boolean> isInitializationThread = ThreadLocal.withInitial(() -> false);

    public static boolean isInitializationThread() {
        return isInitializationThread.get();
    }

    public final static ExecutorService executorService;
    static {
        final ThreadGroup threadGroup = new ThreadGroup("OperationInitializationThreadPool");
        final NamingThreadFactory threadFactory =
                new NamingThreadFactory(threadGroup, OperationInitializationThreadPool.class, "initializationExecutor",
                        true) {
                    @Override
                    public Thread newThread(@NotNull Runnable r) {
                        return super.newThread(() -> {
                            isInitializationThread.set(true);
                            MultiChunkPool.enableDedicatedPoolForThisThread();
                            r.run();
                        });
                    }
                };
        executorService = Executors.newFixedThreadPool(NUM_THREADS, threadFactory);
    }
}
