package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.referencecounting.ReferenceCounted;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * An interface for submitting jobs to be executed. Submitted jobs may be executed on the current thread, or in separate
 * threads (thus allowing true parallelism). Performance metrics are accumulated for all executions off the current
 * thread for inclusion in overall task metrics.
 */
public interface JobScheduler {

    /**
     * A default context for the scheduled job actions. Override this to provide reusable resources for the serial and
     * parallel iterate actions.
     */
    interface JobThreadContext extends Context {
    }

    JobThreadContext DEFAULT_CONTEXT = new JobThreadContext() {};
    Supplier<JobThreadContext> DEFAULT_CONTEXT_FACTORY = () -> DEFAULT_CONTEXT;

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
     * The performance statistics of all runnables that have been completed off-thread, or null if all were executed in
     * the current thread.
     */
    BasePerformanceEntry getAccumulatedPerformance();

    /**
     * How many threads exist in the job scheduler? The job submitters can use this value to determine how many sub-jobs
     * to split work into.
     */
    int threadCount();

    /**
     * Helper interface for {@code iterateSerial()} and {@code iterateParallel()}. This provides a functional interface
     * with {@code index} indicating which iteration to perform. When this returns, the scheduler will automatically
     * schedule the next iteration.
     */
    @FunctionalInterface
    interface IterateAction<CONTEXT_TYPE extends JobThreadContext> {
        void run(CONTEXT_TYPE taskThreadContext, int index);
    }

    /**
     * Helper interface for {@link #iterateSerial} and {@link #iterateParallel}. This provides a functional interface
     * with {@code index} indicating which iteration to perform and {@link Runnable resume} providing a mechanism to
     * inform the scheduler that the current task is complete. When {@code resume} is called, the scheduler will
     * automatically schedule the next iteration.
     * <p>
     * NOTE: failing to call {@code resume} will result in the scheduler not scheduling all remaining iterations. This
     * will not block the scheduler, but the {@code completeAction} {@link Runnable} will never be called.
     */
    @FunctionalInterface
    interface IterateResumeAction<CONTEXT_TYPE extends JobThreadContext> {
        void run(CONTEXT_TYPE taskThreadContext, int index, Runnable resume);
    }

    class ErrorAccounter<CONTEXT_TYPE extends JobThreadContext> extends ReferenceCounted
            implements Consumer<Exception>, Runnable {
        private final Supplier<CONTEXT_TYPE> taskThreadContextFactory;
        private final int start;
        private final int count;
        private final Consumer<Exception> finalErrorConsumer;
        private final IterateResumeAction<CONTEXT_TYPE> action;
        private final AtomicInteger nextIndex;
        private final AtomicInteger remaining;
        private final Runnable resumeAction;

        private final AtomicReference<Exception> exception = new AtomicReference<>();

        ErrorAccounter(final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
                final int start, final int count, final Consumer<Exception> finalErrorConsumer,
                final IterateResumeAction<CONTEXT_TYPE> action, final Runnable completeAction) {
            this.taskThreadContextFactory = taskThreadContextFactory;
            this.start = start;
            this.count = count;
            this.finalErrorConsumer = finalErrorConsumer;
            this.action = action;

            nextIndex = new AtomicInteger(start);
            remaining = new AtomicInteger(count);

            resumeAction = () -> {
                // check for completion
                if (remaining.decrementAndGet() == 0) {
                    completeAction.run();
                }
            };
            // pre-increment this once so we maintain >=1 under normal conditions
            incrementReferenceCount();
        }

        @Override
        protected void onReferenceCountAtZero() {
            final Exception localException = exception.get();
            if (localException != null) {
                finalErrorConsumer.accept(localException);
            }
        }

        @Override
        public void accept(Exception e) {
            exception.compareAndSet(null, e);
            decrementReferenceCount();
        }

        @Override
        public void run() {
            if (!tryIncrementReferenceCount()) {
                // We started this task thread after all sub-tasks are complete or there was an error; we should
                // not try to allocate a task thread context.
                return;
            }
            try (final CONTEXT_TYPE taskThreadContext = taskThreadContextFactory.get()) {
                decrementReferenceCount();
                while (true) {
                    if (exception.get() != null) {
                        return;
                    }
                    final int idx = nextIndex.getAndIncrement();
                    if (idx >= start + count) {
                        return;
                    }
                    if (!tryIncrementReferenceCount()) {
                        // We raced with the exception consumer
                        return;
                    }
                    try {
                        // do the work
                        action.run(taskThreadContext, idx, resumeAction);
                    } finally {
                        decrementReferenceCount();
                    }
                }
            }
        }
    }

    /**
     * Provides a mechanism to iterate over a range of values in parallel using the {@link JobScheduler}
     *
     * @param executionContext the execution context for this task
     * @param description the description to use for logging
     * @param taskThreadContextFactory the factory that supplies {@link JobThreadContext contexts} for the threads
     *        handling the sub-tasks
     * @param start the integer value from which to start iterating
     * @param count the number of times this task should be called
     * @param action the task to perform, the current iteration index is provided as a parameter
     * @param completeAction this will be called when all iterations are complete
     * @param onError error handler for the scheduler to use while iterating
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void iterateParallel(
            ExecutionContext executionContext,
            LogOutputAppendable description,
            Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            int start,
            int count,
            IterateAction<CONTEXT_TYPE> action,
            Runnable completeAction,
            Consumer<Exception> onError) {
        iterateParallel(executionContext, description, taskThreadContextFactory, start, count,
                (final CONTEXT_TYPE taskThreadContext, final int idx, final Runnable resume) -> {
                    action.run(taskThreadContext, idx);
                    resume.run();
                },
                completeAction, onError);
    }

    /**
     * Provides a mechanism to iterate over a range of values in parallel using the {@link JobScheduler}. The advantage
     * to using this over the other method is the resumption callable on {@code action} that will trigger the next
     * execution. This allows the next iteration and the completion runnable to be delayed until dependent asynchronous
     * serial or parallel scheduler jobs have completed.
     *
     * @param executionContext the execution context for this task
     * @param description the description to use for logging
     * @param taskThreadContextFactory the factory that supplies {@link JobThreadContext contexts} for the tasks
     * @param start the integer value from which to start iterating
     * @param count the number of times this task should be called
     * @param action the task to perform, the current iteration index and a resume Runnable are parameters
     * @param completeAction this will be called when all iterations are complete
     * @param onError error handler for the scheduler to use while iterating
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void iterateParallel(
            ExecutionContext executionContext,
            LogOutputAppendable description,
            Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            int start,
            int count,
            IterateResumeAction<CONTEXT_TYPE> action,
            Runnable completeAction,
            Consumer<Exception> onError) {

        if (count == 0) {
            // no work to do
            completeAction.run();
        }

        final ErrorAccounter<CONTEXT_TYPE> ea =
                new ErrorAccounter<>(taskThreadContextFactory, start, count, onError, action, completeAction);

        // create multiple tasks but not more than one per scheduler thread
        for (int i = 0; i < Math.min(count, threadCount()); i++) {
            submit(executionContext,
                    ea,
                    description,
                    ea);
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
     * @param taskThreadContextFactory the factory that supplies {@link JobThreadContext contexts} for the tasks
     * @param start the integer value from which to start iterating
     * @param count the number of times this task should be called
     * @param action the task to perform, the current iteration index and a resume Runnable are parameters
     * @param completeAction this will be called when all iterations are complete
     * @param onError error handler for the scheduler to use while iterating
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void iterateSerial(ExecutionContext executionContext,
            LogOutputAppendable description,
            Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            int start,
            int count,
            IterateResumeAction<CONTEXT_TYPE> action,
            Runnable completeAction,
            Consumer<Exception> onError) {

        if (count == 0) {
            // no work to do
            completeAction.run();
        }

        // create a single execution context for all iterations
        final CONTEXT_TYPE taskThreadContext = taskThreadContextFactory.get();

        final Consumer<Exception> localError = exception -> {
            taskThreadContext.close();
            onError.accept(exception);
        };

        // no lambda, need the `this` reference to re-execute
        final Runnable resumeAction = new Runnable() {
            int nextIndex = start + 1;
            int remaining = count;

            @Override
            public void run() {
                // check for completion
                if (--remaining == 0) {
                    taskThreadContext.close();
                    completeAction.run();
                } else {

                    // schedule the next task
                    submit(executionContext,
                            () -> {
                                int idx = nextIndex++;
                                // do the work
                                action.run(taskThreadContext, idx, this);
                            },
                            description,
                            localError);
                }

            }
        };

        // create a single task
        submit(executionContext,
                () -> action.run(taskThreadContext, start, resumeAction),
                description,
                localError);
    }
}
