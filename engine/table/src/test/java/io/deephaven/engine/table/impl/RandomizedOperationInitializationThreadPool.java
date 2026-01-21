//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.util.thread.ThreadInitializationFactory;

import java.util.Random;
import java.util.concurrent.Future;

/**
 * A version of the operation initialization thread pool that sleeps for a random amount of time before executing the
 * operation initializer; this is intended for unit tests to allow some perturbation for parallel tasks to get better
 * coverage.
 */
public class RandomizedOperationInitializationThreadPool extends OperationInitializationThreadPool {
    final Random random = new Random();
    final int sleepMillis;

    /**
     * Crate the thread pool.
     * 
     * @param factory the thread initialization factory
     * @param sleepMillis the maximum number of milliseconds to sleep before executing the operation initializer. Values
     *        are randomly distributed between 0 and this value.
     */
    public RandomizedOperationInitializationThreadPool(ThreadInitializationFactory factory, int sleepMillis) {
        super(factory);
        this.sleepMillis = sleepMillis;
    }

    @Override
    public Future<?> submit(Runnable runnable) {
        return super.submit(() -> {
            final int toSleep = random.nextInt(sleepMillis);
            try {
                if (toSleep > 0) {
                    Thread.sleep(toSleep);
                }
            } catch (InterruptedException ignored) {
            }
            runnable.run();
        });
    }
}
