//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.util.thread.NamingThreadFactory;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Base implementation of OperationInitializer that delegates to a pool of threads.
 */
class OperationInitializationThreadPoolBase implements OperationInitializer {

    private final ThreadLocal<Boolean> isInitializationThread = ThreadLocal.withInitial(() -> false);

    private final ThreadPoolExecutor executorService;

    private final int numThreads;

    OperationInitializationThreadPoolBase(ThreadInitializationFactory factory, int numThreads, String threadGroupName) {
        this.numThreads = numThreads;
        final ThreadGroup threadGroup = new ThreadGroup(threadGroupName);
        final ThreadFactory threadFactory = new NamingThreadFactory(
                threadGroup, OperationInitializationThreadPoolBase.class, "initializationExecutor", true) {
            @Override
            public Thread newThread(@NotNull final Runnable r) {
                return super.newThread(factory.createInitializer(() -> {
                    isInitializationThread.set(true);
                    MultiChunkPool.enableDedicatedPoolForThisThread();
                    ExecutionContext.newBuilder().setOperationInitializer(OperationInitializer.NON_PARALLELIZABLE)
                            .build().apply(r);
                }));
            }
        };
        executorService = new ThreadPoolExecutor(
                numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);

        executorService.prestartAllCoreThreads();
    }

    @Override
    public boolean canParallelize() {
        return numThreads > 1 && !isInitializationThread.get();
    }

    @Override
    public Future<?> submit(Runnable runnable) {
        return executorService.submit(runnable);
    }

    @Override
    public int parallelismFactor() {
        return numThreads;
    }

    @Override
    public final void shutdownNow() {
        executorService.shutdownNow();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("concurrentDelegate not shutdown within 5 seconds");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for shutdown", e);
        }
    }
}
