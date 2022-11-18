package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.util.annotations.FinalDefault;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * An interface for submitting jobs to be executed and accumulating their performance of all the tasks performed off
 * thread.
 */
public interface JobScheduler {
    /**
     * Cause runnable to be executed.
     *
     * @param executionContext the execution context to run it under
     * @param runnable the runnable to execute
     * @param description a description for logging
     * @param onError a routine to call if an exception occurs while running runnable
     */
    void submit(
            ExecutionContext executionContext,
            Runnable runnable,
            final LogOutputAppendable description,
            final Consumer<Exception> onError);

    /**
     * The performance statistics of all runnables that have been completed off-thread, or null if it was executed in
     * the current thread.
     */
    BasePerformanceEntry getAccumulatedPerformance();

    /**
     * How many threads exist in the job scheduler? The job submitters can use this value to determine how many sub-jobs
     * to split work into.
     */
    int threadCount();

    /**
     * Helper interface for {@code iterateSerial()} and {@code iterateParallel()}. This provides a callable interface
     * with {@code index} indicating which iteration to perform. When this returns, the scheduler will automatically
     * schedule the next iteration.
     */
    @FunctionalInterface
    interface IterateAction {
        void run(int index);
    }

    /**
     * Helper interface for {@code iterateSerial()} and {@code iterateParallel()}. This provides a callable interface
     * with {@code index} indicating which iteration to perform and {@link Runnable resume} providing a mechanism to
     * inform the scheduler that the current task is complete. When {@code resume} is called, the scheduler will
     * automatically schedule the next iteration.
     * <p>
     * NOTE: failing to call {@code resume} will result in the scheduler not scheduling all remaining iterations. This
     * will not block the scheduler, but the {@code completeAction} {@link Runnable} will never be called
     */
    @FunctionalInterface
    interface IterateResumeAction {
        void run(int index, Runnable resume);
    }

    /**
     * Provides a mechanism to iterate over a range of values in parallel using the {@link JobScheduler}
     *
     * @param executionContext the execution context for this task
     * @param description the description to use for logging
     * @param start the integer value from which to start iterating
     * @param count the number of times this task should be called
     * @param action the task to perform, the current iteration index is provided as a parameter
     * @param completeAction this will be called when all iterations are complete
     * @param onError error handler for the scheduler to use while iterating
     */
    @FinalDefault
    default void iterateParallel(ExecutionContext executionContext, LogOutputAppendable description, int start,
            int count, IterateAction action, Runnable completeAction, final Consumer<Exception> onError) {

        if (count == 0) {
            // no work to do
            completeAction.run();
        }

        final AtomicInteger nextIndex = new AtomicInteger(start);
        final AtomicInteger remaining = new AtomicInteger(count);

        final AtomicBoolean cancelRemainingExecution = new AtomicBoolean(false);

        final Consumer<Exception> localError = exception -> {
            cancelRemainingExecution.set(true);
            onError.accept(exception);
        };

        final Runnable task = () -> {
            // this will run until all tasks have started
            while (true) {
                if (cancelRemainingExecution.get()) {
                    return;
                }
                int idx = nextIndex.getAndIncrement();
                if (idx < start + count) {
                    // do the work
                    action.run(idx);

                    // check for completion
                    if (remaining.decrementAndGet() == 0) {
                        completeAction.run();
                        return;
                    }
                } else {
                    // no more work to do
                    return;
                }
            }
        };

        // create multiple tasks but not more than one per scheduler thread
        for (int i = 0; i < Math.min(count, threadCount()); i++) {
            submit(executionContext,
                    task,
                    description,
                    localError);
        }
    }

    /**
     * Provides a mechanism to iterate over a range of values in parallel using the {@link JobScheduler}. The advantage
     * to using this over the other method is the resumption callable on {@code action} that will trigger the next
     * execution. This allows the next iteration and the completion runnable to be delayed until dependent asynchronous
     * serial or parallel scheduler jobs have completed.
     *
     * @param executionContext the execution context for this task
     * @param description the description to use for logging
     * @param start the integer value from which to start iterating
     * @param count the number of times this task should be called
     * @param action the task to perform, the current iteration index and a resume Runnable are parameters
     * @param completeAction this will be called when all iterations are complete
     * @param onError error handler for the scheduler to use while iterating
     */
    @FinalDefault
    default void iterateParallel(ExecutionContext executionContext, LogOutputAppendable description, int start,
            int count, IterateResumeAction action, Runnable completeAction, Consumer<Exception> onError) {

        if (count == 0) {
            // no work to do
            completeAction.run();
        }

        final AtomicInteger nextIndex = new AtomicInteger(start);
        final AtomicInteger remaining = new AtomicInteger(count);

        final AtomicBoolean cancelRemainingExecution = new AtomicBoolean(false);

        final Consumer<Exception> localError = exception -> {
            cancelRemainingExecution.set(true);
            onError.accept(exception);
        };

        final Runnable resumeAction = () -> {
            // check for completion
            if (remaining.decrementAndGet() == 0) {
                completeAction.run();
            }
        };

        final Runnable task = () -> {
            // this will run until all tasks have started
            while (true) {
                if (cancelRemainingExecution.get()) {
                    return;
                }
                int idx = nextIndex.getAndIncrement();
                if (idx < start + count) {
                    // do the work
                    action.run(idx, resumeAction);
                } else {
                    // no more work to do
                    return;
                }
            }
        };

        // create multiple tasks but not more than one per scheduler thread
        for (int i = 0; i < Math.min(count, threadCount()); i++) {
            submit(executionContext,
                    task,
                    description,
                    localError);
        }
    }

    /**
     * Provides a mechanism to iterate over a range of values serially using the {@link JobScheduler}. The advantage to
     * using this over a simple iteration is the resumption callable on {@code action} that will trigger the next
     * execution. This allows the next iteration and the completion runnable to be delayed until dependent asynchronous
     * serial or parallel scheduler jobs have completed.
     *
     * @param executionContext the execution context for this task
     * @param description the description to use for logging
     * @param start the integer value from which to start iterating
     * @param count the number of times this task should be called
     * @param action the task to perform, the current iteration index and a resume Runnable are parameters
     * @param completeAction this will be called when all iterations are complete
     * @param onError error handler for the scheduler to use while iterating
     */
    @FinalDefault
    default void iterateSerial(ExecutionContext executionContext, LogOutputAppendable description, int start,
            int count, IterateResumeAction action, Runnable completeAction, Consumer<Exception> onError) {

        if (count == 0) {
            // no work to do
            completeAction.run();
        }

        final AtomicInteger nextIndex = new AtomicInteger(start);
        final AtomicInteger remaining = new AtomicInteger(count);

        final AtomicBoolean cancelRemainingExecution = new AtomicBoolean(false);

        final Consumer<Exception> localError = exception -> {
            cancelRemainingExecution.set(true);
            onError.accept(exception);
        };

        // no lambda, need the `this` reference to re-execute
        final Runnable resumeAction = new Runnable() {
            @Override
            public void run() {
                // check for completion
                if (remaining.decrementAndGet() == 0) {
                    completeAction.run();
                } else {
                    if (cancelRemainingExecution.get()) {
                        return;
                    }

                    // schedule the next task
                    submit(executionContext,
                            () -> {
                                int idx = nextIndex.getAndIncrement();
                                if (idx < start + count) {
                                    // do the work
                                    action.run(idx, this);
                                }
                            },
                            description,
                            localError);
                }

            }
        };

        // create a single task
        submit(executionContext,
                () -> {
                    int idx = nextIndex.getAndIncrement();
                    if (idx < start + count) {
                        action.run(idx, resumeAction);
                    }
                },
                description,
                localError);
    }
}
