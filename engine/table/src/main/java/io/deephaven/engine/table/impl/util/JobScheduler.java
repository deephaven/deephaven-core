//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.referencecounting.ReferenceCounted;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * An interface for submitting jobs to be executed. Submitted jobs may be executed on the current thread, or in separate
 * threads (thus allowing true parallelism). Performance metrics are accumulated for all executions off the current
 * thread for inclusion in overall task metrics.
 *
 * <p>
 * The iteration methods come in two forms. {@link #iterateParallel} and {@link #iterateSerial} report the outcome
 * through callbacks and may return before the iteration is over, although a scheduler that runs jobs on the submitting
 * thread can finish it first. {@link #invokeParallel} and {@link #invokeSerial} hold the calling thread, which runs
 * tasks alongside the scheduler's threads, and return or throw only once the iteration is over.
 * </p>
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
     * Delivered when a job fails with an {@link Error} and even the wrapper for it cannot be allocated, which is to say
     * when the heap is exhausted — the very failure this path exists for. Allocated once, when this interface is
     * initialized, so that delivering a failure never depends on being able to allocate.
     */
    Exception UNREPORTABLE_JOB_ERROR = new UncheckedDeephavenException(
            "Scheduled job failed with an Error that could not be wrapped for delivery", null, true, false);

    /**
     * Convert a Throwable that escaped a scheduled job into something the {@code Consumer<Exception>} error handlers
     * used throughout the scheduler can accept. Exceptions pass through unchanged; an {@link Error} — an
     * {@link OutOfMemoryError}, in practice — is wrapped, so that the thread waiting on the job's completion fails with
     * a diagnostic instead of waiting forever for a completion that cannot happen.
     *
     * <p>
     * This never throws. The wrapper carries no stack trace of its own: filling one in is the largest allocation here,
     * and the stack that matters belongs to the Error, which is kept as the cause. Should even that allocation fail,
     * {@link #UNREPORTABLE_JOB_ERROR} is delivered instead — a caller that fails without a diagnostic is still far
     * better than one that waits forever.
     * </p>
     *
     * @param throwable the Throwable that escaped the job
     * @return {@code throwable} itself if it is an Exception, otherwise a wrapper holding it as its cause
     */
    static Exception asDeliverableException(@NotNull final Throwable throwable) {
        if (throwable instanceof Exception) {
            return (Exception) throwable;
        }
        try {
            return new UncheckedDeephavenException("Error thrown by scheduled job", throwable, true, false);
        } catch (Throwable t) {
            return UNREPORTABLE_JOB_ERROR;
        }
    }

    /**
     * Run a submitted job the way every scheduler must, on whatever thread the scheduler has chosen for it: under
     * {@code executionContext} when one is given; with an {@link Exception} it throws delivered to {@code onError}; and
     * with an {@link Error} delivered as {@link #asDeliverableException} makes it, then reported to the global fatal
     * error reporter, then rethrown. Implementations wrap this in whatever performance accounting they keep.
     *
     * @param executionContext the execution context to run the job under, or null to run it under the thread's own
     * @param runnable the job
     * @param description a description of the job, for the fatal error report
     * @param onError the consumer to deliver a failure to
     */
    static void runJob(
            @Nullable final ExecutionContext executionContext,
            @NotNull final Runnable runnable,
            @Nullable final LogOutputAppendable description,
            @NotNull final Consumer<Exception> onError) {
        try (final SafeCloseable ignored = executionContext == null ? null : executionContext.open()) {
            runnable.run();
        } catch (Exception e) {
            onError.accept(e);
        } catch (Error e) {
            // Deliver the error before reporting it. Anything waiting on this job's completion has no other way to
            // learn that the job failed, and would otherwise wait forever for a completion that cannot happen.
            try {
                onError.accept(asDeliverableException(e));
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            final String logMessage = new LogOutputStringImpl().append(description).append(" Error").toString();
            ProcessEnvironment.getGlobalFatalErrorReporter().report(logMessage, e);
            throw e;
        }
    }

    /**
     * Cause runnable to be executed.
     *
     * <p>
     * A scheduler runs every job it accepts: at once, after queuing it, or, when it has no thread free and no queue to
     * hold the job, on the calling thread. A scheduler that cannot take a job at all throws instead, and the job does
     * not run.
     * </p>
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
     * Confirm that a thread may block in {@link #invokeParallel} or {@link #invokeSerial} on this scheduler, waiting
     * for the jobs submitted there, by the iteration itself or by nested work its tasks start. That is safe only when
     * every job this scheduler accepts is sure to start on some thread while the caller waits. The default allows it. A
     * scheduler whose jobs run on the very threads that might be the ones waiting, such as the update graph's,
     * overrides this to throw.
     *
     * @throws UnsupportedOperationException if a thread must not block on this scheduler
     */
    default void checkInvokeSupported() {}

    /**
     * The performance statistics of all runnables that have been completed off-thread, or null if all were executed in
     * the current thread.
     *
     * <p>
     * When initializing an operation, the {@link OperationInitializerJobScheduler} executes the completion callback as
     * part of another task. Therefore, you must not read the accumulated performance from the completion callback when
     * initializing an operation; as it could miss data from some of the tasks. Furthermore, even though the completion
     * callback identifies the result as ready does not mean that the completion callback has actually completed. To
     * guard against this the {@link OperationInitializerJobScheduler} waits for all jobs to be complete before
     * returning the {@link BasePerformanceEntry}. Therefore, if you call this from a completion callback, then the
     * operation will hang.
     * </p>
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

    final class IterationManager<CONTEXT_TYPE extends JobThreadContext> extends ReferenceCounted
            implements LogOutputAppendable {

        private static void onUnexpectedJobError(@NotNull final Exception exception) {
            ProcessEnvironment.getGlobalFatalErrorReporter().report("Unexpected iteration job error", exception);
        }

        private final LogOutputAppendable description;
        private final int start;
        private final int count;
        private final IterateResumeAction<CONTEXT_TYPE> action;
        private final Runnable onComplete;
        private final Runnable cleanup;
        private final Consumer<Exception> onError;

        private final AtomicInteger nextAvailableTaskIndex;
        private final AtomicInteger remainingTaskCount;
        private final AtomicReference<Exception> exception;

        IterationManager(
                @Nullable final LogOutputAppendable description,
                final int start,
                final int count,
                @NotNull final IterateResumeAction<CONTEXT_TYPE> action,
                @NotNull final Runnable onComplete,
                @NotNull final Runnable cleanup,
                @NotNull final Consumer<Exception> onError) {
            this.description = description;
            this.start = start;
            this.count = count;
            this.action = action;
            this.onComplete = onComplete;
            this.cleanup = cleanup;
            this.onError = onError;

            nextAvailableTaskIndex = new AtomicInteger(start);
            remainingTaskCount = new AtomicInteger(count);
            exception = new AtomicReference<>();
        }

        /**
         * Start the iteration: make up to {@code min(maxThreads, scheduler.threadCount())} task invokers, each with a
         * context of its own, and submit them to the scheduler. When {@code callerParticipates}, the last of them runs
         * on the calling thread inside this call instead, so that the caller does its share of the work, and so that at
         * least one invoker runs even if the scheduler has no thread to spare.
         *
         * <p>
         * A failure to start, from the context factory or from the scheduler refusing a submission, is recorded as the
         * iteration's failure before it propagates, so that the iteration ends in {@code onError} rather than in
         * {@code onComplete}. The invokers already submitted still finish the tasks they hold.
         * </p>
         */
        private void startTasks(
                @NotNull final JobScheduler scheduler,
                @Nullable final ExecutionContext executionContext,
                @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
                final int maxThreads,
                final boolean callerParticipates) {
            // Increment this once in order to maintain >=1 until all tasks have been submitted
            incrementReferenceCount();
            // every invoker made here: the caller's own, when it takes part, then those submitted to the scheduler
            final List<TaskInvoker> invokers = new ArrayList<>();
            try {
                final int numTaskInvokers = Math.min(maxThreads, scheduler.threadCount());
                final int numSubmitted = callerParticipates ? numTaskInvokers - 1 : numTaskInvokers;
                if (callerParticipates) {
                    // Reserve the caller's own task before any helper can start, so that the caller always takes part
                    // even when a helper would otherwise drain the whole range first.
                    final TaskInvoker own = makeTaskInvoker(taskThreadContextFactory, Math.max(0, numSubmitted));
                    if (own != null) {
                        invokers.add(own);
                    }
                }
                for (int tii = 0; tii < numSubmitted; ++tii) {
                    final TaskInvoker taskInvoker = makeTaskInvoker(taskThreadContextFactory, tii);
                    if (taskInvoker == null) {
                        break;
                    }
                    invokers.add(taskInvoker);
                    scheduler.submit(executionContext, taskInvoker::startAndExecute, description,
                            IterationManager::onUnexpectedJobError);
                }
                if (callerParticipates) {
                    // The caller has to wait, might as well do work rather than idling. Will do its own invoker first,
                    // then look for a submitted one that has not started yet.
                    try (final SafeCloseable ignored = executionContext == null ? null : executionContext.open()) {
                        for (final TaskInvoker taskInvoker : invokers) {
                            if (taskInvoker.tryStart()) {
                                taskInvoker.execute();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                onTaskError(e);
                abandonUnstarted(invokers);
                throw e;
            } catch (Error e) {
                if (exception.get() == null) {
                    onTaskError(asDeliverableException(e));
                }
                abandonUnstarted(invokers);
                throw e;
            } finally {
                decrementReferenceCount();
            }
        }

        /**
         * After a failure to start the iteration, which has already been recorded: close every invoker made for it that
         * has not started, releasing its context and its reference, so that the iteration ends without waiting for jobs
         * a queueing scheduler may start late, or never, if they are queued behind this thread. One that the scheduler
         * starts later finds itself taken and does nothing.
         */
        private void abandonUnstarted(@NotNull final List<TaskInvoker> invokers) {
            for (final TaskInvoker taskInvoker : invokers) {
                if (taskInvoker.tryStart()) {
                    taskInvoker.closeIfOpen();
                }
            }
        }

        /**
         * @return an invoker holding the next task, a context of its own, and a reference to this manager; or
         *         {@code null} when no task is left to hold or the iteration has already failed
         */
        @Nullable
        private TaskInvoker makeTaskInvoker(
                @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
                final int invokerIndex) {
            final int initialTaskIndex = nextAvailableTaskIndex.getAndIncrement();
            if (initialTaskIndex >= start + count || exception.get() != null) {
                return null;
            }
            final CONTEXT_TYPE context = taskThreadContextFactory.get();
            if (!tryIncrementReferenceCount()) {
                context.close();
                return null;
            }
            return new TaskInvoker(context, invokerIndex, initialTaskIndex);
        }

        private void onTaskComplete() {
            if (remainingTaskCount.decrementAndGet() == 0) {
                Assert.eqNull(exception.get(), "exception.get()");
            }
        }

        private void onTaskError(@NotNull final Exception e) {
            recordFailure(exception, e);
        }

        /**
         * Record a failure in {@code holder}: the first one recorded stays, and later ones are added to it as
         * suppressed, so that none is lost.
         */
        private static void recordFailure(
                @NotNull final AtomicReference<Exception> holder,
                @NotNull final Exception e) {
            if (holder.compareAndSet(null, e)) {
                return;
            }
            final Exception first = holder.get();
            // UNREPORTABLE_JOB_ERROR is shared by every iteration, so it must not collect suppressed exceptions
            if (first != e && first != UNREPORTABLE_JOB_ERROR) {
                first.addSuppressed(e);
            }
        }

        @Override
        protected void onReferenceCountAtZero() {
            final Exception localException = exception.get();
            if (localException != null) {
                invokeOnError(localException);
                return;
            }
            try {
                onComplete.run();
            } catch (Exception e) {
                invokeOnError(e);
                return;
            } catch (Error e) {
                // Deliver before rethrowing; this is the operation's only notification that the iteration failed.
                invokeOnError(asDeliverableException(e));
                throw e;
            }
            try {
                cleanup.run();
            } catch (Exception e) {
                onUnexpectedJobError(e);
            }
        }

        private void invokeOnError(@NotNull final Exception exception) {
            try {
                onError.accept(exception);
            } catch (Exception e) {
                e.addSuppressed(exception);
                onUnexpectedJobError(e);
            }
        }

        /**
         * Run an iteration on behalf of a thread that holds on until it is over: start it with the caller taking part,
         * on up to {@code maxThreads} threads the caller's included, wait for it to finish, and report its outcome by
         * returning or throwing. See {@link JobScheduler#invokeParallel} for the contract; {@code maxThreads} of one is
         * {@link JobScheduler#invokeSerial}.
         */
        static <CONTEXT_TYPE extends JobThreadContext> void invoke(
                @NotNull final JobScheduler scheduler,
                @Nullable final ExecutionContext executionContext,
                @Nullable final LogOutputAppendable description,
                @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
                final int maxThreads,
                final int start,
                final int count,
                @NotNull final IterateResumeAction<CONTEXT_TYPE> action) {
            final Invocation invocation = new Invocation();
            // The iteration ends in exactly one of: cleanup (after a success), or onError on whichever thread finishes
            // it; each records the outcome and releases the caller.
            final IterationManager<CONTEXT_TYPE> iterationManager = new IterationManager<>(
                    description, start, count, action,
                    () -> {
                    },
                    invocation::finish,
                    e -> {
                        invocation.fail(e);
                        invocation.finish();
                    });
            Error error = null;
            try {
                iterationManager.startTasks(scheduler, executionContext, taskThreadContextFactory, maxThreads, true);
            } catch (Exception e) {
                // startTasks recorded this as the iteration's failure already; it is thrown below
                invocation.fail(e);
            } catch (Error e) {
                // The invoker or startTasks has recorded this already, so the iteration will end. Wait for it before
                // letting the error go: it is about to unwind past whatever the tasks still running are using.
                error = e;
            }
            invocation.awaitFinished();
            if (error != null && invocation.failedWith(error)) {
                // the iteration failed with this Error, which a task threw on this thread
                throw error;
            }
            // Otherwise the iteration had already failed when this thread threw it, and that first failure is thrown.
            invocation.rethrowFailure();
        }

        /**
         * What a thread blocked in {@link #invoke} waits on, and the outcome it finds when it wakes.
         */
        private static final class Invocation {

            private final CountDownLatch finished = new CountDownLatch(1);
            private final AtomicReference<Exception> failure = new AtomicReference<>();

            private void fail(@NotNull final Exception e) {
                recordFailure(failure, e);
            }

            private void finish() {
                finished.countDown();
            }

            /**
             * Wait for the iteration to finish. Interruption cannot cut the wait short, because the caller is about to
             * release what the running tasks are using; the interrupt is restored once the wait is over.
             */
            private void awaitFinished() {
                boolean interrupted = false;
                while (true) {
                    try {
                        finished.await();
                        break;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }

            /** @return whether the iteration's first recorded failure is {@code error}, or the wrapper delivering it */
            private boolean failedWith(@NotNull final Error error) {
                final Exception first = failure.get();
                return first == null || first.getCause() == error || first == UNREPORTABLE_JOB_ERROR;
            }

            private void rethrowFailure() {
                final Exception thrown = failure.get();
                if (thrown == null) {
                    return;
                }
                if (thrown instanceof RuntimeException) {
                    throw (RuntimeException) thrown;
                }
                throw new UncheckedDeephavenException("Invoked iteration failed", thrown);
            }
        }

        @Override
        public LogOutput append(@NotNull final LogOutput logOutput) {
            return logOutput.append(description)
                    .append("-IterationManager[start=").append(start)
                    .append(",count=").append(count)
                    .append(",nextAvailableTaskIndex=").append(nextAvailableTaskIndex.get())
                    .append(",remainingTaskCount=").append(remainingTaskCount.get())
                    .append(",exceptionSet=").append(exception.get() != null)
                    .append(']');
        }

        private class TaskInvoker implements LogOutputAppendable {

            private final CONTEXT_TYPE context;
            private final int invokerIndex;

            private int acquiredTaskIndex;

            private boolean closed;
            private boolean running;
            /** Set by whichever thread runs this invoker first: the scheduler's, or a blocked caller taking it over. */
            private final AtomicBoolean started = new AtomicBoolean();

            /**
             * Construct a TaskInvoker which will iteratively reschedule itself to perform parallel tasks as needed.
             * This constructor "transfers ownership" to a single reference count on the enclosing IterationManager to
             * the result TaskInvoker, to be released on error or work exhaustion.
             *
             * @param context The context to be used for all tasks performed by this TaskInvoker
             * @param invokerIndex The index of this TaskInvoker within the IterationManager, for debugging and logging
             *        purposes
             * @param initialTaskIndex The index of the initial task to perform
             */
            private TaskInvoker(
                    @NotNull final CONTEXT_TYPE context,
                    final int invokerIndex,
                    final int initialTaskIndex) {
                this.context = context;
                this.invokerIndex = invokerIndex;
                acquiredTaskIndex = initialTaskIndex;
            }

            /** @return whether this thread is the one to run this invoker */
            private boolean tryStart() {
                return started.compareAndSet(false, true);
            }

            /** The job a scheduler runs: this invoker, unless a blocked caller has already taken it over. */
            private void startAndExecute() {
                if (tryStart()) {
                    execute();
                }
            }

            private synchronized void execute() {
                int runningTaskIndex;
                do {
                    if (exception.get() != null) {
                        // We acquired a task index, but the operation is aborting because some other thread reported
                        // an error.
                        close();
                        return;
                    }
                    runningTaskIndex = acquiredTaskIndex;
                    try {
                        running = true;
                        action.run(
                                context,
                                runningTaskIndex,
                                this::reportError,
                                this::reportTaskCompleteAndResumeIteration);
                    } catch (Exception e) {
                        deliverTaskFailure(e);
                        return;
                    } catch (Error e) {
                        // An Error -- an OutOfMemoryError, in practice -- has to be delivered before it propagates.
                        // Letting it escape undelivered would skip close(), leaving this TaskInvoker's reference to
                        // the IterationManager outstanding, so that the reference count never reaches zero and
                        // neither onComplete nor onError ever runs. Rethrow once it has been delivered, so that the
                        // scheduler still reports it as fatal and it reaches the thread running this job.
                        deliverTaskFailure(asDeliverableException(e));
                        throw e;
                    } finally {
                        running = false;
                    }
                } while (runningTaskIndex != acquiredTaskIndex && !closed);
            }

            private void deliverTaskFailure(@NotNull final Exception e) {
                if (closed) {
                    // The task threw an error while trying to deliver another error or complete the iteration.
                    // We cannot safely deliver this error, but we don't want to allow incorrect operation, so
                    // we report it to the global error reporter.
                    onUnexpectedJobError(e);
                } else {
                    // Something went wrong, but no completion or error was delivered yet. Report the error.
                    reportError(e);
                }
            }

            private synchronized void reportTaskCompleteAndResumeIteration() {
                // This might be called from the original thread that ran our action for acquiredTaskIndex, *or* from
                // a thread that completed that task asynchronously. Regardless, we always try to acquire a new task,
                // freeing our resources if there are no tasks remaining or an error was reported asynchronously.
                // If we *do* have a task to execute, if we're on the original thread (running == true) we return in
                // order to allow the enclosing loop to execute our task in an orderly fashion without any recursion
                // in the thread stack, else we run it here, hijacking the thread that reported the prior iteration's
                // completion.
                onTaskComplete();
                if ((acquiredTaskIndex = nextAvailableTaskIndex.getAndIncrement()) >= start + count
                        || exception.get() != null) {
                    close();
                } else if (!running) {
                    execute();
                }
            }

            private synchronized void reportError(@NotNull final Exception e) {
                try (final SafeCloseable ignored = this::close) {
                    onTaskError(Objects.requireNonNull(e));
                }
            }

            private void close() {
                Assert.eqFalse(closed, "closed");
                try (final SafeCloseable ignored = context) {
                    closed = true;
                } finally {
                    decrementReferenceCount();
                }
            }

            /** Close this invoker unless it has run and closed itself; for an invoker the scheduler never took. */
            private synchronized void closeIfOpen() {
                if (!closed) {
                    close();
                }
            }

            @Override
            public LogOutput append(@NotNull final LogOutput logOutput) {
                return logOutput.append(IterationManager.this)
                        .append("-TaskInvoker[invokerIndex=").append(invokerIndex)
                        .append(",acquiredTaskIndex=").append(acquiredTaskIndex)
                        .append(",closed=").append(closed)
                        .append(']');
            }

            @Override
            public String toString() {
                return new LogOutputStringImpl().append(this).toString();
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
     * @param cleanup called after onComplete successfully returns. If the invocation of the cleanup throws an
     *        exception, onError will <em>not</em> be called.
     * @param onError error handler for the scheduler to use while iterating, or if onComplete throws an exception.
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void iterateParallel(
            @Nullable final ExecutionContext executionContext,
            @Nullable final LogOutputAppendable description,
            @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            final int start,
            final int count,
            @NotNull final IterateAction<CONTEXT_TYPE> action,
            @NotNull final Runnable onComplete,
            @NotNull final Runnable cleanup,
            @NotNull final Consumer<Exception> onError) {
        iterateParallel(executionContext, description, taskThreadContextFactory, start, count,
                (final CONTEXT_TYPE taskThreadContext,
                        final int taskIndex,
                        final Consumer<Exception> nestedErrorConsumer,
                        final Runnable resume) -> {
                    action.run(taskThreadContext, taskIndex, nestedErrorConsumer);
                    resume.run();
                }, onComplete, cleanup, onError);
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
     * @param cleanup called after onComplete successfully returns. If the invocation of the cleanup throws an
     *        exception, onError will <em>not</em> be called.
     * @param onError error handler for the scheduler to use while iterating, or if onComplete throws an exception.
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void iterateParallel(
            @Nullable final ExecutionContext executionContext,
            @Nullable final LogOutputAppendable description,
            @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            final int start,
            final int count,
            @NotNull final IterateResumeAction<CONTEXT_TYPE> action,
            @NotNull final Runnable onComplete,
            @NotNull final Runnable cleanup,
            @NotNull final Consumer<Exception> onError) {
        final IterationManager<CONTEXT_TYPE> iterationManager =
                new IterationManager<>(description, start, count, action, onComplete, cleanup, onError);
        iterationManager.startTasks(this, executionContext, taskThreadContextFactory, count, false);
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
     * @param cleanup called after onComplete successfully returns. If the invocation of the cleanup throws an
     *        exception, onError will <em>not</em> be called.
     * @param onError error handler for the scheduler to use while iterating, or if onComplete throws an exception.
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void iterateSerial(
            @Nullable final ExecutionContext executionContext,
            @Nullable final LogOutputAppendable description,
            @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            final int start,
            final int count,
            @NotNull final IterateResumeAction<CONTEXT_TYPE> action,
            @NotNull final Runnable onComplete,
            @NotNull final Runnable cleanup,
            @NotNull final Consumer<Exception> onError) {
        final IterationManager<CONTEXT_TYPE> iterationManager =
                new IterationManager<>(description, start, count, action, onComplete, cleanup, onError);
        iterationManager.startTasks(this, executionContext, taskThreadContextFactory, 1, false);
    }

    /**
     * Iterates over a range of values in parallel as {@link #iterateParallel} does, except that the calling thread runs
     * tasks too, and this method returns only once the iteration is over. See the
     * {@link #invokeParallel(ExecutionContext, LogOutputAppendable, Supplier, int, int, IterateResumeAction) resume
     * form} for the contract.
     *
     * @param executionContext the execution context the tasks run under, on every thread that runs them, the calling
     *        thread's included; null to run them under each thread's own
     * @param description the description to use for logging
     * @param taskThreadContextFactory the factory that supplies {@link JobThreadContext contexts} for the tasks
     * @param start the integer value from which to start iterating
     * @param count the number of times this task should be called
     * @param action the task to perform, the current iteration index is provided as a parameter
     * @throws UnsupportedOperationException if this scheduler does not allow a thread to block on it
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void invokeParallel(
            @Nullable final ExecutionContext executionContext,
            @Nullable final LogOutputAppendable description,
            @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            final int start,
            final int count,
            @NotNull final IterateAction<CONTEXT_TYPE> action) {
        invokeParallel(executionContext, description, taskThreadContextFactory, start, count,
                (final CONTEXT_TYPE taskThreadContext,
                        final int taskIndex,
                        final Consumer<Exception> nestedErrorConsumer,
                        final Runnable resume) -> {
                    action.run(taskThreadContext, taskIndex, nestedErrorConsumer);
                    resume.run();
                });
    }

    /**
     * Iterates over a range of values in parallel as {@link #iterateParallel} does, except that the calling thread runs
     * tasks too, and this method returns only once the iteration is over. Where {@code iterateParallel} reports its
     * outcome through callbacks and may return first, this holds the calling thread and reports its outcome by
     * returning or throwing; whatever a caller would do in {@code onComplete}, {@code cleanup} or {@code onError} it
     * does after this call, or in a {@code catch} or {@code finally} around it.
     *
     * <p>
     * <b>Participation.</b> Up to {@code min(count, threadCount()) - 1} task invokers are submitted to the scheduler,
     * and one more runs on the calling thread, each with a task context of its own from
     * {@code taskThreadContextFactory}, which is called on the calling thread. The caller therefore works even when the
     * scheduler accepts every submission. When the caller runs out of tasks it runs any submitted invoker that the
     * scheduler has not started yet, which then does nothing when the scheduler does start it; so the caller never
     * waits on an invoker that has not started, whatever the scheduler does with a job it cannot start at once.
     * </p>
     *
     * <p>
     * <b>Return.</b> This returns only once every task has completed and every task context has been closed. A task may
     * hand its completion to nested asynchronous work through {@code resume}, in which case the thread that finishes
     * that work carries the iteration on, and the caller waits for it here.
     * </p>
     *
     * <p>
     * <b>Failure.</b> A task that throws, or that reports a failure through its nested error consumer, fails the
     * iteration: no new task starts, and once every task already running has finished, the first failure is thrown, as
     * itself when it is a {@link RuntimeException} and wrapped in an {@link UncheckedDeephavenException} otherwise.
     * Later failures are attached to it as suppressed. An {@link Error} thrown by a task on a scheduler thread is still
     * reported as fatal and rethrown there, and reaches the caller wrapped, as {@link #asDeliverableException} makes
     * it; one thrown on the calling thread reaches the caller as itself, once the other tasks have finished.
     * </p>
     *
     * <p>
     * <b>Interruption.</b> The wait cannot be interrupted, because the caller is about to release what the running
     * tasks are using; the interrupt is restored before this returns.
     * </p>
     *
     * <p>
     * <b>Schedulers.</b> A thread may block here only on a scheduler whose accepted jobs are sure to start while it
     * waits; see {@link #checkInvokeSupported()}, which an unsuitable scheduler makes throw before anything runs.
     * </p>
     *
     * @param executionContext the execution context the tasks run under, on every thread that runs them, the calling
     *        thread's included; null to run them under each thread's own
     * @param description the description to use for logging
     * @param taskThreadContextFactory the factory that supplies {@link JobThreadContext contexts} for the tasks
     * @param start the integer value from which to start iterating
     * @param count the number of times this task should be called
     * @param action the task to perform, the current iteration index and a resume Runnable are parameters
     * @throws UnsupportedOperationException if this scheduler does not allow a thread to block on it
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void invokeParallel(
            @Nullable final ExecutionContext executionContext,
            @Nullable final LogOutputAppendable description,
            @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            final int start,
            final int count,
            @NotNull final IterateResumeAction<CONTEXT_TYPE> action) {
        checkInvokeSupported();
        IterationManager.invoke(this, executionContext, description, taskThreadContextFactory, count, start, count,
                action);
    }

    /**
     * Iterates over a range of values serially as {@link #iterateSerial} does, except that the calling thread starts
     * the steps and runs each one that the step before it completed on this thread, and this method returns only once
     * the iteration is over. The steps run one at a time, in order, and the next begins only once the previous has
     * called {@code resume}. A step may hand its completion to nested work that runs in parallel beneath it, such as an
     * {@link #iterateParallel} on this scheduler given {@code resume} as its completion and the nested error consumer
     * as its error handler; the thread that finishes that work then runs the next step, and the caller waits here for
     * the whole chain.
     *
     * <p>
     * In every other respect, the return point, what is thrown, interruption, and which schedulers allow it, the
     * contract is that of
     * {@link #invokeParallel(ExecutionContext, LogOutputAppendable, Supplier, int, int, IterateResumeAction)
     * invokeParallel}. Nothing is submitted to the scheduler by the iteration itself, only by whatever nested work the
     * steps start, which is why a scheduler that refuses {@code invokeParallel} refuses this too.
     * </p>
     *
     * @param executionContext the execution context the tasks run under, on every thread that runs them, the calling
     *        thread's included; null to run them under each thread's own
     * @param description the description to use for logging
     * @param taskThreadContextFactory the factory that supplies the one {@link JobThreadContext context} the steps
     *        share
     * @param start the integer value from which to start iterating
     * @param count the number of times this task should be called
     * @param action the step to perform, the current iteration index and a resume Runnable are parameters
     * @throws UnsupportedOperationException if this scheduler does not allow a thread to block on it
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void invokeSerial(
            @Nullable final ExecutionContext executionContext,
            @Nullable final LogOutputAppendable description,
            @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            final int start,
            final int count,
            @NotNull final IterateResumeAction<CONTEXT_TYPE> action) {
        checkInvokeSupported();
        IterationManager.invoke(this, executionContext, description, taskThreadContextFactory, 1, start, count,
                action);
    }
}
