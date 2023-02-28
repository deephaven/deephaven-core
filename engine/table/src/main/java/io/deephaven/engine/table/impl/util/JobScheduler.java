package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.referencecounting.ReferenceCounted;
import org.jetbrains.annotations.NotNull;

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
        /**
         * Iteration action to be invoked.
         *
         * @param taskThreadContext The context, unique to this task-thread
         * @param index The iteration number
         * @param nestedErrorConsumer A consumer to pass to directly-nested iterative jobs
         */
        void run(CONTEXT_TYPE taskThreadContext, int index, Consumer<Exception> nestedErrorConsumer);
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
        /**
         * Iteration action to be invoked.
         *
         * @param taskThreadContext The context, unique to this task-thread
         * @param index The iteration number
         * @param nestedErrorConsumer A consumer to pass to directly-nested iterative jobs
         * @param resume A function to call to move on to the next iteration
         */
        void run(CONTEXT_TYPE taskThreadContext, int index, Consumer<Exception> nestedErrorConsumer, Runnable resume);
    }

    final class IterationManager<CONTEXT_TYPE extends JobThreadContext> extends ReferenceCounted {

        private final int start;
        private final int count;
        private final Consumer<Exception> onError;
        private final IterateResumeAction<CONTEXT_TYPE> action;
        private final Runnable onComplete;

        private final AtomicInteger nextAvailableTaskIndex;
        private final AtomicInteger remainingTaskCount;
        private final AtomicReference<Exception> exception;

        IterationManager(final int start,
                final int count,
                @NotNull final Consumer<Exception> onError,
                @NotNull final IterateResumeAction<CONTEXT_TYPE> action,
                @NotNull final Runnable onComplete) {
            this.start = start;
            this.count = count;
            this.onError = onError;
            this.action = action;
            this.onComplete = onComplete;

            nextAvailableTaskIndex = new AtomicInteger(start);
            remainingTaskCount = new AtomicInteger(count);
            exception = new AtomicReference<>();
        }

        private void startTasks(
                @NotNull final JobScheduler scheduler,
                @NotNull final ExecutionContext executionContext,
                @NotNull final LogOutputAppendable description,
                @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
                final int maxThreads) {
            // Increment this once in order to maintain >=1 until completed
            incrementReferenceCount();
            final int numTaskInvokers = Math.min(maxThreads, scheduler.threadCount());
            for (int tii = 0; tii < numTaskInvokers; ++tii) {
                final int initialTaskIndex = nextAvailableTaskIndex.getAndIncrement();
                if (initialTaskIndex >= start + count || exception.get() != null) {
                    break;
                }
                final CONTEXT_TYPE context = taskThreadContextFactory.get();
                if (!tryIncrementReferenceCount()) {
                    context.close();
                    break;
                }
                final TaskInvoker taskInvoker = new TaskInvoker(context, initialTaskIndex);
                scheduler.submit(executionContext, taskInvoker, description, taskInvoker);
            }
        }

        private void onTaskComplete() {
            if (remainingTaskCount.decrementAndGet() == 0) {
                Assert.eqNull(exception.get(), "exception.get()");
                decrementReferenceCount();
            }
        }

        private void onTaskError(@NotNull final Exception e) {
            if (exception.compareAndSet(null, e)) {
                decrementReferenceCount();
            }
        }

        @Override
        protected void onReferenceCountAtZero() {
            final Exception localException = exception.get();
            if (localException != null) {
                onError.accept(localException);
            } else {
                onComplete.run();
            }
        }

        private class TaskInvoker implements Runnable, Consumer<Exception>, SafeCloseable {

            private final CONTEXT_TYPE context;

            private volatile boolean closed;

            private int acquiredTaskIndex;
            private boolean running;

            /**
             * Construct a TaskInvoker which will iteratively reschedule itself to perform parallel tasks as needed.
             * This constructor "transfers ownership" to a single reference count on the enclosing IterationManager to
             * the result TaskInvoker, to be released on error or work exhaustion.
             *
             * @param context The context to be used for all tasks performed by this TaskInvoker
             * @param initialTaskIndex The index of the initial task to perform
             */
            private TaskInvoker(@NotNull final CONTEXT_TYPE context, final int initialTaskIndex) {
                this.context = context;
                acquiredTaskIndex = initialTaskIndex;
            }

            @Override
            public synchronized void run() {
                int runningTaskIndex;
                do {
                    if (exception.get() != null) {
                        // We acquired a task index, but the operation is aborting
                        close();
                        return;
                    }
                    runningTaskIndex = acquiredTaskIndex;
                    running = true;
                    action.run(context, runningTaskIndex, this, this::onTaskDone);
                    running = false;
                } while (runningTaskIndex != acquiredTaskIndex && !closed);
            }

            private synchronized void onTaskDone() {
                onTaskComplete();
                if ((acquiredTaskIndex = nextAvailableTaskIndex.getAndIncrement()) >= start + count
                        || exception.get() != null) {
                    close();
                } else if (!running) {
                    run();
                }
            }

            @Override
            public void accept(@NotNull final Exception e) {
                try (final SafeCloseable ignored = this) {
                    onTaskError(e);
                }
            }

            @Override
            public void close() {
                Assert.eqFalse(closed, "closed");
                try (final SafeCloseable ignored = context) {
                    closed = true;
                } finally {
                    decrementReferenceCount();
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
     * @param onComplete this will be called when all iterations are complete
     * @param onError error handler for the scheduler to use while iterating
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void iterateParallel(
            @NotNull final ExecutionContext executionContext,
            @NotNull final LogOutputAppendable description,
            @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            final int start,
            final int count,
            @NotNull final IterateAction<CONTEXT_TYPE> action,
            @NotNull final Runnable onComplete,
            @NotNull final Consumer<Exception> onError) {
        iterateParallel(executionContext, description, taskThreadContextFactory, start, count,
                (final CONTEXT_TYPE taskThreadContext,
                        final int taskIndex,
                        final Consumer<Exception> nestedErrorConsumer,
                        final Runnable resume) -> {
                    action.run(taskThreadContext, taskIndex, nestedErrorConsumer);
                    resume.run();
                },
                onComplete, onError);
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
     * @param onComplete this will be called when all iterations are complete
     * @param onError error handler for the scheduler to use while iterating
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void iterateParallel(
            @NotNull final ExecutionContext executionContext,
            @NotNull final LogOutputAppendable description,
            @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            final int start,
            final int count,
            @NotNull final IterateResumeAction<CONTEXT_TYPE> action,
            @NotNull final Runnable onComplete,
            @NotNull final Consumer<Exception> onError) {

        if (count == 0) {
            // no work to do
            onComplete.run();
        }

        final IterationManager<CONTEXT_TYPE> iterationManager =
                new IterationManager<>(start, count, onError, action, onComplete);
        iterationManager.startTasks(this, executionContext, description, taskThreadContextFactory, count);
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
     * @param onComplete this will be called when all iterations are complete
     * @param onError error handler for the scheduler to use while iterating
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void iterateSerial(
            @NotNull final ExecutionContext executionContext,
            @NotNull final LogOutputAppendable description,
            @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            final int start,
            final int count,
            @NotNull final IterateResumeAction<CONTEXT_TYPE> action,
            @NotNull final Runnable onComplete,
            @NotNull final Consumer<Exception> onError) {

        if (count == 0) {
            // no work to do
            onComplete.run();
        }

        final IterationManager<CONTEXT_TYPE> iterationManager =
                new IterationManager<>(start, count, onError, action, onComplete);
        iterationManager.startTasks(this, executionContext, description, taskThreadContextFactory, 1);
    }
}
