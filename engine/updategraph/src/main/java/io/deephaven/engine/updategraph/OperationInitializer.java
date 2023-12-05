package io.deephaven.engine.updategraph;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * alt naming: OperationParallelismControl?
 */
public interface OperationInitializer {
    OperationInitializer NON_PARALLELIZABLE = new OperationInitializer() {
        @Override
        public boolean canParallelize() {
            return false;
        }

        @Override
        public Future<?> submit(Runnable runnable) {
            runnable.run();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public int parallelismFactor() {
            return 0;
        }

        @Override
        public void start() {
            // no-op
        }
    };

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
