package io.deephaven.db.v2;

import io.deephaven.configuration.Configuration;
import io.deephaven.util.thread.NamingThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TableMapTransformThreadPool {
    final static int TRANSFORM_THREADS =
            Configuration.getInstance().getIntegerWithDefault("TableMap.transformThreads", 1);

    final static ExecutorService executorService;
    static {
        final ThreadGroup threadGroup = new ThreadGroup("TableMapTransformThreadPool");
        final NamingThreadFactory threadFactory =
                new NamingThreadFactory(threadGroup, TableMapProxyHandler.class, "transformExecutor", true);
        executorService = Executors.newFixedThreadPool(TRANSFORM_THREADS, threadFactory);
    }
}
