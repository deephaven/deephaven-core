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

import static io.deephaven.util.thread.ThreadHelpers.getOrComputeThreadCountProperty;

/**
 * Implementation of OperationInitializer that delegates to a pool of threads.
 */
public class OperationInitializationThreadPool implements OperationInitializer {

    /**
     * The number of threads that will be used for parallel initialization in this process
     */
    private static final int NUM_THREADS =
            getOrComputeThreadCountProperty("OperationInitializationThreadPool.threads", -1);
    private final ThreadLocal<Boolean> isInitializationThread = ThreadLocal.withInitial(() -> false);

    private final ThreadPoolExecutor executorService;

    public OperationInitializationThreadPool(ThreadInitializationFactory factory) {
        final ThreadGroup threadGroup = new ThreadGroup("OperationInitializationThreadPool");
        final ThreadFactory threadFactory = new NamingThreadFactory(
                threadGroup, OperationInitializationThreadPool.class, "initializationExecutor", true) {
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
                NUM_THREADS, NUM_THREADS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);

        executorService.prestartAllCoreThreads();
    }

    @Override
    public boolean canParallelize() {
        return NUM_THREADS > 1 && !isInitializationThread.get();
    }

    @Override
    public Future<?> submit(Runnable runnable) {
        return executorService.submit(runnable);
    }

    @Override
    public int parallelismFactor() {
        return NUM_THREADS;
    }
}
