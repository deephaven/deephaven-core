package io.deephaven.engine.table.impl;

import io.deephaven.configuration.Configuration;
import io.deephaven.util.thread.NamingThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OperationInitializationThreadPool {
    final static int TRANSFORM_THREADS =
            Configuration.getInstance().getIntegerWithDefault("OperationInitializationThreadPool.threads", 1);

    public final static ExecutorService executorService;
    static {
        final ThreadGroup threadGroup = new ThreadGroup("OperationInitializationThreadPool");
        final NamingThreadFactory threadFactory =
                new NamingThreadFactory(threadGroup, OperationInitializationThreadPool.class, "initializationExecutor",
                        true);
        executorService = Executors.newFixedThreadPool(TRANSFORM_THREADS, threadFactory);
    }
}
