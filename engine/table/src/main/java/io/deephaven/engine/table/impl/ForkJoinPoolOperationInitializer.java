//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.OperationInitializer;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * Implementation of {@link OperationInitializer} that delegates to a {@link ForkJoinPool}.
 */
public class ForkJoinPoolOperationInitializer implements OperationInitializer {

    @NotNull
    public static OperationInitializer fromCommonPool() {
        return COMMON;
    }

    private static final ForkJoinPoolOperationInitializer COMMON =
            new ForkJoinPoolOperationInitializer(ForkJoinPool.commonPool()) {

                private final ExecutionContext executionContext = ExecutionContext.newBuilder()
                        .setOperationInitializer(NON_PARALLELIZABLE)
                        .build();

                @Override
                public @NotNull Future<?> submit(@NotNull final Runnable task) {
                    return super.submit(() -> executionContext.apply(task));
                }
            };

    private final ForkJoinPool pool;

    private ForkJoinPoolOperationInitializer(@NotNull final ForkJoinPool pool) {
        this.pool = Objects.requireNonNull(pool);
    }

    @Override
    public boolean canParallelize() {
        return parallelismFactor() > 1 && ForkJoinTask.getPool() != pool;
    }

    @Override
    @NotNull
    public Future<?> submit(@NotNull final Runnable taskRunnable) {
        return pool.submit(taskRunnable);
    }

    @Override
    public int parallelismFactor() {
        return pool.getParallelism();
    }

    /**
     * Ensure that {@code task} is parallelizable within the current {@link ExecutionContext}, by wrapping it with a new
     * {@code ExecutionContext} that uses {@link #fromCommonPool()} if the current {@code ExecutionContext} does not
     * {@link OperationInitializer#canParallelize() allow parallelization}.
     *
     * @param task The task to possible wrap
     * @return The possibly-wrapped task
     */
    public static Runnable ensureParallelizable(@NotNull final Runnable task) {
        if (ExecutionContext.getContext().getOperationInitializer().canParallelize()) {
            return task;
        }
        return () -> ExecutionContext.getContext()
                .withOperationInitializer(ForkJoinPoolOperationInitializer.fromCommonPool())
                .apply(task);
    }

    /**
     * Ensure that {@code task} is parallelizable within the current {@link ExecutionContext}, by wrapping it with a new
     * {@code ExecutionContext} that uses {@link #fromCommonPool()} if the current {@code ExecutionContext} does not
     * {@link OperationInitializer#canParallelize() allow parallelization}.
     *
     * @param task The task to possible wrap
     * @return The possibly-wrapped task
     */
    public static <T> Supplier<T> ensureParallelizable(@NotNull final Supplier<T> task) {
        if (ExecutionContext.getContext().getOperationInitializer().canParallelize()) {
            return task;
        }
        return () -> ExecutionContext.getContext()
                .withOperationInitializer(ForkJoinPoolOperationInitializer.fromCommonPool())
                .apply(task);
    }
}
