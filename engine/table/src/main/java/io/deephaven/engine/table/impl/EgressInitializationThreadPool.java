//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.util.thread.ThreadInitializationFactory;

import static io.deephaven.util.thread.ThreadHelpers.getOrComputeThreadCountProperty;

public class EgressInitializationThreadPool extends OperationInitializationThreadPoolBase
        implements OperationInitializer {

    /**
     * Generating snapshots for GUI can lead to blocking reads from disk, so we want to have a wider threads available
     **/
    private static final int DEFAULT_NUM_THREADS = 2 * Runtime.getRuntime().availableProcessors();

    /**
     * The number of threads that will be used for parallel initialization in this process
     */
    private static final int NUM_THREADS =
            getOrComputeThreadCountProperty("EgressInitializationThreadPool.threads", -1, DEFAULT_NUM_THREADS);

    public EgressInitializationThreadPool(final ThreadInitializationFactory factory) {
        super(factory, NUM_THREADS, "EgressInitializationThreadPool");
    }
}
