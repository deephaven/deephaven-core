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
 * thread can finish it first. {@link #invokeParallel} holds the calling thread, which runs tasks alongside the
 * scheduler's threads, and returns or throws only once the iteration is over.
 * </p>
 *
 * <p>
 * An iteration fails when a task throws, when an {@link IterateResumeAction} task reports a failure through its nested
 * error consumer, when a task context fails to close, or when it cannot be started because the context factory or
 * {@link #submit} throws. No new task starts once it has failed. The callback forms then call {@code onError} with the
 * first failure, and neither {@code onComplete} nor {@code cleanup}; a failure to start reaches their caller only that
 * way, unless it is not an {@link Exception}, an {@link Error} in practice, which is also rethrown.
 * {@code invokeParallel} throws the first failure instead, as it describes. Later failures are suppressed on the first.
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
     * initialized, so that delivering a failure never depends on being able to allocate. Being shared, it takes no
     * suppressed exceptions.
     */
    Exception UNREPORTABLE_JOB_ERROR = new UncheckedDeephavenException(
            "Scheduled job failed with an Error that could not be wrapped for delivery", null, false, false);

    /**
     * Convert a Throwable that escaped a scheduled job into something the {@code Consumer<Exception>} error handlers
     * used throughout the scheduler can accept. Exceptions pass through unchanged; anything else is wrapped, so that
     * the thread waiting on the job's completion fails with a diagnostic instead of waiting forever for a completion
     * that cannot happen. That is an {@link Error} in practice, most often an {@link OutOfMemoryError}, though Groovy
     * code can throw a plain {@link Throwable} as well.
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
     * Run a submitted job on whatever thread the scheduler has chosen for it: under {@code executionContext} when one
     * is given; with an {@link Exception} it throws delivered to {@code onError}; and with anything else it throws, an
     * {@link Error} in practice, delivered as {@link #asDeliverableException} makes it, then reported to the global
     * fatal error reporter, then rethrown. The default reporter ends the process and does not return, so the rethrow
     * happens only under a reporter that does. Implementations wrap this in whatever performance accounting they keep.
     * {@link ImmediateJobScheduler}, which runs every job on the submitting thread, does not use it: an Error from one
     * of its jobs propagates to that thread undelivered and unreported.
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
        } catch (Throwable e) {
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
     * operation will hang. {@link ExecutorJobScheduler} waits the same way, so the same applies to it.
     * </p>
     */
    BasePerformanceEntry getAccumulatedPerformance();

    /**
     * How many threads exist in the job scheduler? The job submitters can use this value to determine how many sub-jobs
     * to split work into.
     */
    int threadCount();

    /**
     * Helper interface for {@link JobScheduler#iterateParallel} and {@link JobScheduler#invokeParallel}. This provides
     * a functional interface with {@code index} indicating which iteration to perform. When this returns, the scheduler
     * will automatically schedule the next iteration.
     *
     * <p>
     * A task in this form fails by throwing, and should not call its nested error consumer: since the iteration moves
     * on as soon as the task returns, a failure reported there is fatal unless the task then throws that same failure,
     * and {@link JobScheduler#invokeParallel} may return before the task does. Nor should it hand the consumer to
     * nested work that it then waits on: a call to the consumer from another thread waits for the task to return, so
     * that work failing deadlocks the task. Nested work that completes asynchronously needs
     * {@link IterateResumeAction}.
     * </p>
     */
    @FunctionalInterface
    interface IterateAction<CONTEXT_TYPE extends JobThreadContext> {
        /**
         * Iteration action to be invoked.
         *
         * @param taskThreadContext The context, unique to this task-thread
         * @param index The iteration number
         * @param nestedErrorConsumer A consumer that a task in this form should not call; see {@link IterateAction}
         */
        void run(CONTEXT_TYPE taskThreadContext, int index, Consumer<Exception> nestedErrorConsumer);
    }

    /**
     * Helper interface for {@link #iterateSerial} and {@link #iterateParallel}. This provides a functional interface
     * with {@code index} indicating which iteration to perform and {@link Runnable resume} providing a mechanism to
     * inform the scheduler that the current task is complete. When {@code resume} is called, the scheduler will
     * automatically schedule the next iteration.
     * <p>
     * NOTE: failing to call {@code resume} will result in the scheduler not scheduling all remaining iterations. That
     * does not block the scheduler, but the iteration never ends: neither {@code onComplete} nor {@code onError} is
     * ever called.
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
        /**
         * Failures to close a task context, kept apart from {@link #exception} because they can arrive after every task
         * has completed; merged into it once the last reference is released.
         */
        private final AtomicReference<Exception> contextCloseFailure;

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
            contextCloseFailure = new AtomicReference<>();
        }

        /**
         * Start the iteration: make up to {@code min(maxThreads, scheduler.threadCount())} task invokers, each with a
         * context of its own, and submit them to the scheduler. When {@code callerParticipates}, the first of them is
         * reserved for the calling thread before any is submitted, and is not submitted; the caller runs it inside this
         * call, then any submitted invoker the scheduler has not started yet. So the caller does its share of the work,
         * at least one invoker runs even if the scheduler has no thread to spare, and the caller never waits on an
         * invoker that has not started.
         *
         * <p>
         * A failure to start, from the context factory or from the scheduler refusing a submission, is recorded as the
         * iteration's failure, so that the iteration ends in {@code onError} rather than in {@code onComplete}. It is
         * not thrown, though anything that is not an {@link Exception}, an {@link Error} in practice, is rethrown once
         * recorded. The refused invoker is closed, since nothing else will run it; see {@link #abandon} for the others.
         * A submission that throws after its job already ran on this thread did not refuse it, and its exception is the
         * caller's, thrown once {@link #abandon} has closed the invokers that have not started.
         * </p>
         *
         * @param scheduler the scheduler to submit the invokers to
         * @param executionContext the execution context the tasks run under, or null to run them under each thread's
         *        own
         * @param taskThreadContextFactory makes each invoker's task context, on the calling thread
         * @param maxThreads the most invokers to make, further capped by the scheduler's thread count
         * @param callerParticipates true for {@code invokeParallel}, whose caller runs the first invoker itself; false
         *        for the callback forms, which submit every invoker
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
            final ArrayList<TaskInvoker> invokers = new ArrayList<>();
            // the invoker being handed to the scheduler, while submit is running
            TaskInvoker submitting = null;
            try {
                final int numTaskInvokers = Math.min(maxThreads, scheduler.threadCount());
                // Sized before any invoker holds a reference: an add that failed to allocate would strand one
                invokers.ensureCapacity(numTaskInvokers);
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
                    submitting = taskInvoker;
                    scheduler.submit(executionContext, taskInvoker::startAndExecute, description,
                            IterationManager::onUnexpectedJobError);
                    submitting = null;
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
                if (submitting != null && !submitting.tryStart()) {
                    // The invoker started before submit threw, so the scheduler ran its job inline and did not refuse
                    // it. A started invoker owns its own lifecycle and delivered any failure of its tasks itself, so
                    // this exception belongs to the caller, who would otherwise wait on its own unstarted invoker.
                    abandon(null, invokers, callerParticipates);
                    throw e;
                }
                // abandon even if recording fails to allocate, or the caller would wait on what it closes
                try {
                    onTaskError(e);
                } finally {
                    abandon(submitting, invokers, callerParticipates);
                }
            } catch (Throwable e) {
                // a task's Error on this thread, or a context's failure to close, was recorded where it was thrown
                final TaskInvoker refused = submitting != null && submitting.tryStart() ? submitting : null;
                try {
                    if (!isRecorded(e)) {
                        onTaskError(asDeliverableException(e));
                    }
                } finally {
                    abandon(refused, invokers, callerParticipates);
                }
                throw e;
            } finally {
                decrementReferenceCount();
            }
        }

        /**
         * After the iteration failed to start, or a submission threw after running its job on this thread: close
         * {@code refused}, which nothing else will run. When the caller takes part, also close every other invoker that
         * has not started, so that the caller does not wait for jobs a queueing scheduler may start late, or never, if
         * they are queued behind this thread; one the scheduler starts later finds itself taken and does nothing.
         * Otherwise the scheduler starts the invokers it accepted, which close at once if the iteration has failed.
         *
         * @param refused the invoker whose submission the scheduler refused, already taken by this thread, or null
         */
        private void abandon(
                @Nullable final TaskInvoker refused,
                @NotNull final List<TaskInvoker> invokers,
                final boolean callerParticipates) {
            if (refused != null) {
                closeAbandoned(refused);
            }
            if (callerParticipates) {
                for (final TaskInvoker taskInvoker : invokers) {
                    if (taskInvoker.tryStart()) {
                        closeAbandoned(taskInvoker);
                    }
                }
            }
        }

        private void closeAbandoned(@NotNull final TaskInvoker taskInvoker) {
            try {
                taskInvoker.closeIfOpen();
            } catch (Throwable e) {
                // close() recorded it, and released the reference; any other invokers still need closing
            }
        }

        /**
         * @return whether {@code failure} was already recorded as a failure of this iteration: itself, or when it is
         *         not an Exception, the wrapper delivering it
         */
        private boolean isRecorded(@NotNull final Throwable failure) {
            return isRecordedIn(exception.get(), failure) || isRecordedIn(contextCloseFailure.get(), failure);
        }

        private static boolean isRecordedIn(@Nullable final Exception first, @NotNull final Throwable failure) {
            if (first == null) {
                return false;
            }
            if (delivers(first, failure)) {
                return true;
            }
            for (final Throwable suppressed : first.getSuppressed()) {
                if (delivers(suppressed, failure)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * @return whether {@code recorded} is {@code failure}, or the wrapper delivering it when it is not an Exception
         */
        private static boolean delivers(@NotNull final Throwable recorded, @NotNull final Throwable failure) {
            return recorded == failure || (!(failure instanceof Exception)
                    && (recorded.getCause() == failure || recorded == UNREPORTABLE_JOB_ERROR));
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
            if (initialTaskIndex >= start + count || hasFailed()) {
                return null;
            }
            final CONTEXT_TYPE context = taskThreadContextFactory.get();
            // made before it takes its reference, which nothing would release if allocating it failed
            final TaskInvoker taskInvoker = new TaskInvoker(context, invokerIndex, initialTaskIndex);
            if (!tryIncrementReferenceCount()) {
                context.close();
                return null;
            }
            return taskInvoker;
        }

        /**
         * @return whether the iteration has failed, so that no new task may start: a failure recorded in
         *         {@link #exception}, or a task context that failed to close
         */
        private boolean hasFailed() {
            return exception.get() != null || contextCloseFailure.get() != null;
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
            if (first != e) {
                first.addSuppressed(e);
            }
        }

        @Override
        protected void onReferenceCountAtZero() {
            final Exception closeFailure = contextCloseFailure.get();
            if (closeFailure != null) {
                try {
                    recordFailure(exception, closeFailure);
                } catch (Throwable t) {
                    // only an allocation can fail here; deliver the failure without it rather than not at all
                }
            }
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
            } catch (Throwable e) {
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
                if (e != exception) {
                    // an onError that rethrows what it was given cannot suppress it on itself
                    e.addSuppressed(exception);
                }
                onUnexpectedJobError(e);
            }
        }

        /**
         * Run an iteration on behalf of a thread that holds on until it is over: start it with the caller taking part,
         * wait for it to finish, and report its outcome by returning or throwing. See
         * {@link JobScheduler#invokeParallel} for the contract.
         */
        static <CONTEXT_TYPE extends JobThreadContext> void invoke(
                @NotNull final JobScheduler scheduler,
                @Nullable final ExecutionContext executionContext,
                @Nullable final LogOutputAppendable description,
                @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
                final int start,
                final int count,
                @NotNull final IterateAction<CONTEXT_TYPE> action) {
            final Invocation invocation = new Invocation();
            // The iteration ends in exactly one of: cleanup (after a success), or onError on whichever thread finishes
            // it; each records the outcome and releases the caller.
            final IterationManager<CONTEXT_TYPE> iterationManager = new IterationManager<>(
                    description, start, count,
                    (final CONTEXT_TYPE taskThreadContext,
                            final int taskIndex,
                            final Consumer<Exception> nestedErrorConsumer,
                            final Runnable resume) -> {
                        action.run(taskThreadContext, taskIndex, nestedErrorConsumer);
                        resume.run();
                    },
                    () -> {
                    },
                    invocation::finish,
                    e -> {
                        try {
                            invocation.fail(e);
                        } finally {
                            invocation.finish();
                        }
                    });
            Exception notRecorded = null;
            try {
                iterationManager.startTasks(scheduler, executionContext, taskThreadContextFactory, count, true);
            } catch (Exception e) {
                // Thrown only by a submission whose job had already run inline, which did not fail the iteration; it
                // is still the caller's, so it is thrown once the iteration is over.
                notRecorded = e;
            } catch (Throwable e) {
                // The invoker or startTasks has recorded this already, so the iteration will end. Wait for it before
                // letting the error go: it is about to unwind past whatever the tasks still running are using.
                invocation.awaitFinished();
                if (!invocation.failedWith(e)) {
                    // the iteration had already failed when this thread threw it, and that first failure is thrown
                    invocation.rethrowFailure();
                }
                // the iteration failed with this, which a task threw on this thread
                invocation.addLaterFailuresTo(e);
                throw e;
            }
            invocation.awaitFinished();
            if (notRecorded != null) {
                invocation.fail(notRecorded);
            }
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
            private boolean failedWith(@NotNull final Throwable error) {
                final Exception first = failure.get();
                return first == null || first.getCause() == error || first == UNREPORTABLE_JOB_ERROR;
            }

            /**
             * Attach to {@code error}, the iteration's first failure, the failures recorded after it, which are
             * suppressed on the wrapper that delivered it.
             */
            private void addLaterFailuresTo(@NotNull final Throwable error) {
                final Exception first = failure.get();
                if (first == null) {
                    return;
                }
                try {
                    for (final Throwable later : first.getSuppressed()) {
                        if (later.getCause() != error) {
                            error.addSuppressed(later);
                        }
                    }
                } catch (Throwable t) {
                    // best effort: with the heap exhausted, the Error is thrown with whatever it carries
                }
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
             * Once made, it is given a single reference count on the enclosing IterationManager, to be released on
             * error or work exhaustion.
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
                    if (hasFailed()) {
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
                    } catch (Throwable e) {
                        // An Error -- an OutOfMemoryError, in practice, though Groovy code can throw a plain Throwable
                        // -- has to be delivered before it propagates.
                        // Letting it escape undelivered would skip close(), leaving this TaskInvoker's reference to
                        // the IterationManager outstanding, so that the reference count never reaches zero and
                        // neither onComplete nor onError ever runs. Rethrow once it has been delivered, so that the
                        // scheduler still reports it as fatal and it reaches the thread running this job.
                        deliverTaskFailure(e);
                        throw e;
                    } finally {
                        running = false;
                    }
                } while (runningTaskIndex != acquiredTaskIndex && !closed);
            }

            private void deliverTaskFailure(@NotNull final Throwable failure) {
                if (!closed) {
                    // Something went wrong, but no completion or error was delivered yet. Report the error.
                    reportError(asDeliverableException(failure));
                } else if (!isRecorded(failure)) {
                    // The task threw an error while trying to deliver another error or complete the iteration.
                    // We cannot safely deliver this error, but we don't want to allow incorrect operation, so
                    // we report it to the global error reporter.
                    onUnexpectedJobError(asDeliverableException(failure));
                }
                // Otherwise it was delivered before it was thrown: through the nested error consumer, as a task that
                // reports a failure and then throws it does, or a nested iteration that fails to start with an Error,
                // or by close().
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
                        || hasFailed()) {
                    close();
                } else if (!running) {
                    execute();
                }
            }

            private synchronized void reportError(@NotNull final Exception e) {
                Objects.requireNonNull(e);
                if (closed) {
                    // A failure the iteration already recorded, arriving a second way, is dropped; any other means
                    // the task reported a failure after it had finished.
                    if (!isRecorded(e)) {
                        onUnexpectedJobError(e);
                    }
                    return;
                }
                // not a try-with-resources, whose resource is allocated before the try: failing that would skip close()
                try {
                    onTaskError(e);
                } finally {
                    close();
                }
            }

            private void close() {
                Assert.eqFalse(closed, "closed");
                closed = true;
                try {
                    context.close();
                } catch (Exception e) {
                    // recorded before the reference is released, so that the iteration cannot end without it
                    recordFailure(contextCloseFailure, e);
                } catch (Throwable e) {
                    recordFailure(contextCloseFailure, asDeliverableException(e));
                    throw e;
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
     * @param onError error handler for the scheduler to use when the iteration fails, as {@link JobScheduler}
     *        describes, or if onComplete throws an exception
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
     * @param onError error handler for the scheduler to use when the iteration fails, as {@link JobScheduler}
     *        describes, or if onComplete throws an exception
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
     * @param onError error handler for the scheduler to use when the iteration fails, as {@link JobScheduler}
     *        describes, or if onComplete throws an exception
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
     * <b>Return.</b> This returns only once every task has completed and every task context has been closed.
     * </p>
     *
     * <p>
     * <b>Failure.</b> A task fails by throwing, as {@link IterateAction} describes, and so does the iteration, as it
     * does when a task context fails to close or the iteration fails to start: no new task starts, and once every task
     * already running has finished, the first failure is thrown, as itself when it is a {@link RuntimeException} and
     * wrapped in an {@link UncheckedDeephavenException} otherwise. Later failures are attached to it as suppressed. An
     * {@link Error}, or any other Throwable that is not an Exception, thrown by a task on a scheduler thread is still
     * reported as fatal and rethrown there, and reaches the caller wrapped, as {@link #asDeliverableException} makes
     * it; one thrown on the calling thread reaches the caller as itself, carrying the later failures, once the other
     * tasks have finished. A submitted invoker that the scheduler runs on the calling thread, as
     * {@link ExecutorJobScheduler} does with a job its executor refuses, is still the scheduler's job, and an Error
     * from its tasks is reported as fatal as it would be on a scheduler thread.
     * </p>
     *
     * <p>
     * <b>Interruption.</b> The wait cannot be interrupted, because the caller is about to release what the running
     * tasks are using; the interrupt is restored before this returns.
     * </p>
     *
     * <p>
     * <b>Nesting and schedulers.</b> The caller waits only on tasks already running on another thread, never on one the
     * scheduler has not started, so a task may itself call {@code invokeParallel}, to any depth, on any scheduler and
     * on whatever thread it runs on. The invocation completes as long as every task's own work does and no task waits
     * on other work submitted to the scheduler, such as a callback-form iteration, which the caller cannot run on its
     * behalf. Nor may a task wait on another task of the same invocation: with one thread, or no pool thread free, the
     * caller runs them one after another. The update graph's scheduler accepts jobs only during an update cycle, so
     * with more than one update thread this must be called during an update cycle. Its parallelism comes from the
     * update graph's pool threads. Called from an update thread, as listener code is, the other tasks are dispatched to
     * the remaining pool threads by the update graph's refresh thread. Called from the refresh thread itself, nothing
     * is dispatched while it waits, so it runs every task itself, one after another.
     * </p>
     *
     * @param executionContext the execution context the tasks run under, on every thread that runs them, the calling
     *        thread's included; null to run them under each thread's own
     * @param description the description to use for logging
     * @param taskThreadContextFactory the factory that supplies {@link JobThreadContext contexts} for the tasks
     * @param start the integer value from which to start iterating
     * @param count the number of times this task should be called
     * @param action the task to perform, the current iteration index is provided as a parameter
     */
    @FinalDefault
    default <CONTEXT_TYPE extends JobThreadContext> void invokeParallel(
            @Nullable final ExecutionContext executionContext,
            @Nullable final LogOutputAppendable description,
            @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            final int start,
            final int count,
            @NotNull final IterateAction<CONTEXT_TYPE> action) {
        IterationManager.invoke(this, executionContext, description, taskThreadContextFactory, start, count, action);
    }
}
