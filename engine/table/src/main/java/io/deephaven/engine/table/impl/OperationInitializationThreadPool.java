//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.util.thread.ThreadInitializationFactory;

import static io.deephaven.util.thread.ThreadHelpers.getOrComputeThreadCountProperty;

public class OperationInitializationThreadPool extends OperationInitializationThreadPoolBase
        implements OperationInitializer {

    /**
     * The number of threads that will be used for parallel initialization in this process
     */
    private static final int NUM_THREADS =
            getOrComputeThreadCountProperty("OperationInitializationThreadPool.threads", -1);

    public OperationInitializationThreadPool(final ThreadInitializationFactory factory) {
        super(factory, NUM_THREADS, "OperationInitializationThreadPool");
    }
}
