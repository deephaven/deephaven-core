//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.OperationInitializer;
import org.jetbrains.annotations.NotNull;

/**
 * An {@link ExecutorJobScheduler} over an {@link OperationInitializer}, whose pool is shared and has a fixed number of
 * threads, and whose {@link OperationInitializer#parallelismFactor() parallelism} is the scheduler's thread count.
 *
 * <p>
 * Each job runs once, on whichever thread claims it first: a pool thread, or a thread waiting in
 * {@link #awaitCompletion}, which runs the jobs no pool thread has started rather than waiting for one to. A waiting
 * thread therefore blocks only while every job it has not finished is running on another thread, and never on a job
 * queued behind pool threads that may themselves be waiting, perhaps for a lock the waiting thread holds. An
 * initializer that runs a job inside {@code submit}, as {@link OperationInitializer#NON_PARALLELIZABLE} does, makes it
 * the submitting thread's own work.
 * </p>
 */
public class OperationInitializerJobScheduler extends ExecutorJobScheduler {

    public OperationInitializerJobScheduler(@NotNull final OperationInitializer operationInitializer) {
        super(operationInitializer::submit, operationInitializer::parallelismFactor);
    }

    public OperationInitializerJobScheduler() {
        this(ExecutionContext.getContext().getOperationInitializer());
    }
}
