package io.deephaven.engine.table.impl;

import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.thread.NamingThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OperationInitializationThreadPool {
    public final static int NUM_THREADS =
            Configuration.getInstance().getIntegerWithDefault("OperationInitializationThreadPool.threads", 1);

    public final static ExecutorService executorService;
    static {
        final ThreadGroup threadGroup = new ThreadGroup("OperationInitializationThreadPool");
        final NamingThreadFactory threadFactory =
                new NamingThreadFactory(threadGroup, OperationInitializationThreadPool.class, "initializationExecutor",
                        true) {
                    @Override
                    public Thread newThread(@NotNull Runnable r) {
                        return super.newThread(() -> {
                            MultiChunkPool.enableDedicatedPoolForThisThread();
                            r.run();
                        });
                    }
                };
        executorService = Executors.newFixedThreadPool(NUM_THREADS, threadFactory);
    }
}
