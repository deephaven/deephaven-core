package io.deephaven.engine.updategraph;

import java.util.concurrent.Future;

/**
 * alt naming: OperationParallelismControl?
 */
public interface OperationInitializer {
    /**
     * @return Whether the current thread can parallelize operations using this OperationInitialization.
     */
    boolean canParallelize();

    /**
     * Submits a task to run in this thread pool.
     * 
     * @param runnable
     * @return
     */
    Future<?> submit(Runnable runnable);

    /**
     *
     * @return
     */
    int parallelismFactor();

    /**
     *
     */
    void start();
}
