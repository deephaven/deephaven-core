//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.QueryTableTestBase;
import io.deephaven.util.SafeCloseable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Harness shared by the concurrent instantiation tests: worker pools that run table operations off the update graph
 * thread, each with the test's {@link ExecutionContext} open.
 */
public abstract class TestConcurrentInstantiationBase extends QueryTableTestBase {
    static final int TIMEOUT_LENGTH = 10;
    static final TimeUnit TIMEOUT_UNIT = TimeUnit.SECONDS;

    ExecutorService pool;
    ExecutorService dualPool;
    ExecutorService largePool;
    ControlledUpdateGraph updateGraph;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final ExecutionContext executionContext = ExecutionContext.makeExecutionContext(true);
        final ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    runnable.run();
                }
            });
            thread.setDaemon(true);
            return thread;
        };
        pool = Executors.newFixedThreadPool(1, threadFactory);
        dualPool = Executors.newFixedThreadPool(2, threadFactory);
        largePool = Executors.newFixedThreadPool(10, threadFactory);
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            pool.shutdown();
            dualPool.shutdown();
            largePool.shutdown();
        }
    }
}
