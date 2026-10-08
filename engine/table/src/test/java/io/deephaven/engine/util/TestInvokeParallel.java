//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.util.ExecutorJobScheduler;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler;
import io.deephaven.engine.table.impl.util.UpdateGraphJobScheduler;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of {@link JobScheduler#invokeParallel} and {@link JobScheduler#invokeSerial}: the calling thread takes part,
 * the call returns only once the iteration is over, and a failure is thrown to the caller.
 */
public class TestInvokeParallel {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    private final List<ThreadPoolExecutor> pools = new ArrayList<>();

    @After
    public void shutDownPools() throws InterruptedException {
        for (final ThreadPoolExecutor pool : pools) {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    /** A pool shaped like the server's: up to {@code maxThreads} threads made on demand, and no queue. */
    private ThreadPoolExecutor newPool(final int maxThreads) {
        final ThreadPoolExecutor pool = ExecutorJobScheduler.newHelperPool(maxThreads, runnable -> {
            final Thread thread = new Thread(runnable, "invoke-parallel-test");
            thread.setDaemon(true);
            return thread;
        });
        pools.add(pool);
        return pool;
    }

    private ExecutorJobScheduler newScheduler(final int poolThreads, final int threadCount) {
        return new ExecutorJobScheduler(newPool(poolThreads), threadCount);
    }

    private static List<Integer> items(final int count) {
        return IntStream.range(0, count).boxed().collect(Collectors.toList());
    }

    private static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void await(final CyclicBarrier barrier) {
        try {
            barrier.await(30, TimeUnit.SECONDS);
        } catch (final Exception e) {
            throw new RuntimeException("the tasks did not all run at once", e);
        }
    }

    /**
     * Runs {@code invocation} on a thread of its own and fails, rather than hangs, if it has not returned in time; for
     * the cases whose failure mode would be an invocation that never returns.
     */
    private static void withTimeout(final Runnable invocation) throws InterruptedException {
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                invocation.run();
            } catch (final Throwable t) {
                thrown.set(t);
            }
        }, "invoke-parallel-test-caller");
        thread.setDaemon(true);
        thread.start();
        thread.join(30_000);
        if (thread.isAlive()) {
            throw new AssertionError("the invocation did not return");
        }
        if (thrown.get() != null) {
            if (thrown.get() instanceof RuntimeException) {
                throw (RuntimeException) thrown.get();
            }
            throw (Error) thrown.get();
        }
    }

    /** Records how a callback-form iteration ended, for the tests of what the two forms share. */
    private static final class Callbacks {
        private final AtomicInteger completeCalls = new AtomicInteger();
        private final AtomicInteger cleanupCalls = new AtomicInteger();
        private final AtomicReference<Exception> error = new AtomicReference<>();

        private final Runnable onComplete = completeCalls::incrementAndGet;
        private final Runnable cleanup = cleanupCalls::incrementAndGet;
        private final Consumer<Exception> onError = e -> {
            if (!error.compareAndSet(null, e)) {
                throw new IllegalStateException("onError called twice");
            }
        };
    }

    /** Invokes {@code count} tasks on {@code scheduler} with the default context. */
    private static void invoke(
            final JobScheduler scheduler,
            final int count,
            final JobScheduler.IterateAction<JobScheduler.JobThreadContext> action) {
        invoke(scheduler, JobScheduler.DEFAULT_CONTEXT_FACTORY, count, action);
    }

    private static <CONTEXT_TYPE extends JobScheduler.JobThreadContext> void invoke(
            final JobScheduler scheduler,
            final Supplier<CONTEXT_TYPE> contextFactory,
            final int count,
            final JobScheduler.IterateAction<CONTEXT_TYPE> action) {
        scheduler.invokeParallel(ExecutionContext.getContext(), logOutput -> logOutput.append("TestInvokeParallel"),
                contextFactory, 0, count, action);
    }

    @Test
    public void testImmediateRunsEveryTaskInOrderOnTheCallingThread() {
        final List<Integer> order = new ArrayList<>();
        final Set<Thread> threads = ConcurrentHashMap.newKeySet();
        invoke(new ImmediateJobScheduler(), 20, (context, idx, nec) -> {
            order.add(idx);
            threads.add(Thread.currentThread());
        });

        assertThat(order).isEqualTo(items(20));
        assertThat(threads).containsExactly(Thread.currentThread());
    }

    /**
     * The tasks rendezvous, which they can do only if threadCount-many of them run at once, the caller's among them.
     */
    @Test
    public void testCallerParticipatesEvenWhenEverySubmissionIsAccepted() {
        final int threadCount = 4;
        // the pool could run every submitted invoker, so only participation puts the caller among the threads
        final ExecutorJobScheduler scheduler = newScheduler(16, threadCount);
        final CyclicBarrier rendezvous = new CyclicBarrier(threadCount);
        final Set<Thread> threads = ConcurrentHashMap.newKeySet();
        invoke(scheduler, threadCount, (context, idx, nec) -> {
            threads.add(Thread.currentThread());
            await(rendezvous);
        });

        assertThat(threads).hasSize(threadCount).contains(Thread.currentThread());
    }

    @Test
    public void testNeverRunsMoreThanThreadCountTasksAtOnce() {
        final int threadCount = 3;
        final ExecutorJobScheduler scheduler = newScheduler(16, threadCount);
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger mostRunning = new AtomicInteger();
        invoke(scheduler, 64, (context, idx, nec) -> {
            mostRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            sleep(2);
            running.decrementAndGet();
        });

        assertThat(mostRunning.get()).isBetween(1, threadCount);
    }

    @Test
    public void testRunsEveryTaskExactlyOnce() {
        final ExecutorJobScheduler scheduler = newScheduler(7, 8);
        for (final int numTasks : new int[] {0, 1, 2, 7, 8, 9, 1000}) {
            final AtomicIntegerArray runs = new AtomicIntegerArray(Math.max(1, numTasks));
            invoke(scheduler, numTasks, (context, idx, nec) -> runs.incrementAndGet(idx));
            for (int ii = 0; ii < numTasks; ++ii) {
                assertThat(runs.get(ii)).as("runs of task %d of %d", ii, numTasks).isEqualTo(1);
            }
        }
    }

    /** Every task must have finished by the time the invocation returns. */
    @Test
    public void testReturnsOnlyAfterEveryTaskHasRun() {
        final ExecutorJobScheduler scheduler = newScheduler(3, 4);
        final AtomicInteger finished = new AtomicInteger();
        invoke(scheduler, 16, (context, idx, nec) -> {
            sleep(5);
            finished.incrementAndGet();
        });

        assertThat(finished.get()).isEqualTo(16);
    }

    /**
     * An executor that refuses every job leaves all the work to the calling thread, which runs every task exactly once:
     * its own invoker's, and the refused ones, which run inline as they are submitted. The order then differs from the
     * index order, since the caller reserves its own task before submitting the others; a scheduler of one thread
     * submits nothing and runs the tasks in order.
     */
    @Test
    public void testRefusingExecutorRunsEverythingOnTheCallingThread() {
        final Executor refusing = command -> {
            throw new RejectedExecutionException("no threads to spare");
        };
        final Set<Thread> threads = ConcurrentHashMap.newKeySet();
        final List<Integer> order = Collections.synchronizedList(new ArrayList<>());
        invoke(new ExecutorJobScheduler(refusing, 8), 20, (context, idx, nec) -> {
            threads.add(Thread.currentThread());
            order.add(idx);
        });

        assertThat(threads).containsExactly(Thread.currentThread());
        assertThat(order).containsExactlyInAnyOrderElementsOf(items(20));
    }

    /** A task may invoke a nested iteration on the same pool even when the pool has no thread left. */
    @Test
    public void testNestedInvokeOnAnExhaustedPoolCompletes() throws InterruptedException {
        final ExecutorJobScheduler scheduler = newScheduler(1, 4);
        final AtomicInteger innerRuns = new AtomicInteger();
        withTimeout(() -> {
            invoke(scheduler, 4, (context, outer, nec) -> invoke(scheduler, 5,
                    (innerContext, inner, innerNec) -> innerRuns.incrementAndGet()));
        });
        assertThat(innerRuns.get()).isEqualTo(20);
    }

    /**
     * A task may instead start nested asynchronous work, handing it {@code resume} and the nested error consumer, and
     * return; the thread that finishes the nested work carries the outer iteration on, and the caller waits for all of
     * it.
     */
    @Test
    public void testNestedAsynchronousIterationCarriesTheOuterIterationOn() throws InterruptedException {
        final ExecutorJobScheduler scheduler = newScheduler(3, 4);
        final AtomicInteger innerRuns = new AtomicInteger();
        final AtomicInteger outerRuns = new AtomicInteger();
        withTimeout(() -> {
            scheduler.invokeParallel(ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0, 12,
                    (context, outer, nestedErrorConsumer, resume) -> {
                        outerRuns.incrementAndGet();
                        scheduler.iterateParallel(ExecutionContext.getContext(), null,
                                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 5,
                                (innerContext, inner, innerNec) -> innerRuns.incrementAndGet(),
                                resume, () -> {
                                }, nestedErrorConsumer);
                    });
        });

        assertThat(outerRuns.get()).isEqualTo(12);
        assertThat(innerRuns.get()).isEqualTo(60);
    }

    @Test
    public void testNestedFailureReportedThroughTheNestedErrorConsumerFailsTheInvocation()
            throws InterruptedException {
        final ExecutorJobScheduler scheduler = newScheduler(1, 2);
        final IllegalStateException failure = new IllegalStateException("nested task failed");
        withTimeout(() -> {
            assertThatThrownBy(() -> scheduler.invokeParallel(ExecutionContext.getContext(), null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 2,
                    (context, outer, nestedErrorConsumer, resume) -> scheduler.iterateParallel(
                            ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 3,
                            (innerContext, inner, innerNec) -> {
                                if (outer == 0 && inner == 1) {
                                    throw failure;
                                }
                            },
                            resume, () -> {
                            }, nestedErrorConsumer)))
                    .isSameAs(failure);
        });

    }

    @Test
    public void testSchedulerThreadsRunUnderTheGivenExecutionContext() {
        final ExecutionContext callerContext = ExecutionContext.getContext();
        final int threadCount = 4;
        final ExecutorJobScheduler scheduler = newScheduler(threadCount - 1, threadCount);
        final CyclicBarrier rendezvous = new CyclicBarrier(threadCount);
        final Set<ExecutionContext> contexts = ConcurrentHashMap.newKeySet();
        invoke(scheduler, threadCount, (context, idx, nec) -> {
            contexts.add(ExecutionContext.getContext());
            await(rendezvous);
        });

        assertThat(contexts).containsExactly(callerContext);
    }

    /**
     * Tasks run under the given execution context on every thread, the calling thread's included, even when it differs
     * from the context the caller is running under.
     */
    @Test
    public void testTasksRunUnderTheGivenContextOnTheCallingThreadToo() {
        final ExecutionContext callerContext = ExecutionContext.getContext();
        final ExecutionContext taskContext = ExecutionContext.newBuilder().newQueryScope().newQueryLibrary()
                .setUpdateGraph(callerContext.getUpdateGraph()).build();
        assertThat(taskContext).isNotSameAs(callerContext);
        final int threadCount = 4;
        final ExecutorJobScheduler scheduler = newScheduler(threadCount - 1, threadCount);
        final CyclicBarrier rendezvous = new CyclicBarrier(threadCount);
        final Set<ExecutionContext> contexts = ConcurrentHashMap.newKeySet();
        final Set<Thread> threads = ConcurrentHashMap.newKeySet();

        scheduler.invokeParallel(taskContext, null, JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, threadCount,
                (context, idx, nec) -> {
                    contexts.add(ExecutionContext.getContext());
                    threads.add(Thread.currentThread());
                    await(rendezvous);
                });

        assertThat(threads).contains(Thread.currentThread());
        assertThat(contexts).containsExactly(taskContext);
        assertThat(ExecutionContext.getContext()).isSameAs(callerContext);
    }

    /** The first failure is thrown once the tasks already running have finished; tasks not yet started are skipped. */
    @Test
    public void testFailureIsThrownAfterRunningTasksFinishAndStopsNewOnes() {
        final int threadCount = 4;
        final ExecutorJobScheduler scheduler = newScheduler(threadCount - 1, threadCount);
        final CyclicBarrier allStarted = new CyclicBarrier(threadCount);
        final AtomicInteger started = new AtomicInteger();
        final AtomicInteger finished = new AtomicInteger();
        final IllegalStateException failure = new IllegalStateException("task failed");

        assertThatThrownBy(() -> scheduler.invokeParallel(ExecutionContext.getContext(), null,
                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 100,
                (context, idx, nec) -> {
                    started.incrementAndGet();
                    await(allStarted);
                    if (idx == 0) {
                        throw failure;
                    }
                    sleep(50);
                    finished.incrementAndGet();
                })).isSameAs(failure);

        // the tasks that were running when task 0 failed all finished; none of the other 96 started
        assertThat(started.get()).isEqualTo(threadCount);
        assertThat(finished.get()).isEqualTo(threadCount - 1);
    }

    @Test
    public void testLaterFailuresAreSuppressedOnTheFirst() {
        final ExecutorJobScheduler scheduler = newScheduler(1, 2);
        final CyclicBarrier bothStarted = new CyclicBarrier(2);
        final IllegalStateException first = new IllegalStateException("first");
        final IllegalArgumentException second = new IllegalArgumentException("second");

        assertThatThrownBy(() -> scheduler.invokeParallel(ExecutionContext.getContext(), null,
                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 2,
                (context, idx, nec) -> {
                    await(bothStarted);
                    if (idx == 0) {
                        throw first;
                    }
                    // fail second, after the first has had time to be recorded
                    sleep(50);
                    throw second;
                }))
                .isSameAs(first)
                .satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(second));
    }

    /** An Error on the calling thread ends the iteration, and is thrown as itself. */
    @Test
    public void testErrorOnTheCallingThreadIsThrownAsItself() {
        final AssertionError error = new AssertionError("task error");
        final List<Integer> ran = new ArrayList<>();

        assertThatThrownBy(() -> new ImmediateJobScheduler().invokeParallel(ExecutionContext.getContext(), null,
                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 3,
                (context, idx, nec) -> {
                    ran.add(idx);
                    if (idx == 1) {
                        throw error;
                    }
                })).isSameAs(error);

        assertThat(ran).containsExactly(0, 1);
    }

    /**
     * An executor that fails a submission outright, rather than refusing it, fails the invocation; but not before the
     * threads it did start have finished the tasks they were running, and without stranding the invoker it failed.
     */
    @Test
    public void testBrokenExecutorFailsTheInvocationAfterRunningTasksFinish() throws InterruptedException {
        final ThreadPoolExecutor pool = newPool(1);
        final AtomicInteger submissions = new AtomicInteger();
        final IllegalStateException broken = new IllegalStateException("cannot start a thread");
        final Executor failsSecondSubmission = command -> {
            if (submissions.incrementAndGet() > 1) {
                throw broken;
            }
            pool.execute(command);
        };
        final AtomicInteger started = new AtomicInteger();
        final AtomicInteger finished = new AtomicInteger();
        final AtomicInteger contextsOpen = new AtomicInteger();
        final Supplier<JobScheduler.JobThreadContext> countingContexts = () -> {
            contextsOpen.incrementAndGet();
            return new JobScheduler.JobThreadContext() {
                @Override
                public void close() {
                    contextsOpen.decrementAndGet();
                }
            };
        };

        withTimeout(() -> {
            assertThatThrownBy(() -> new ExecutorJobScheduler(failsSecondSubmission, 4).invokeParallel(
                    ExecutionContext.getContext(), null, countingContexts, 0, 8,
                    (context, idx, nec) -> {
                        started.incrementAndGet();
                        sleep(100);
                        finished.incrementAndGet();
                    })).isSameAs(broken);
        });

        // Only the thread that did start can have begun a task before the failure was recorded, and whatever it began
        // had finished by the time the invocation threw. Every context, the stranded invoker's included, is closed.
        assertThat(started.get()).isLessThanOrEqualTo(1);
        assertThat(finished.get()).isEqualTo(started.get());
        assertThat(contextsOpen.get()).isZero();
    }

    /**
     * An executor that fails a submission with an Error, as one does when it cannot make a thread, fails the invocation
     * with that Error once the running tasks are done, and releases the invoker it never took, rather than leaving the
     * caller waiting forever.
     */
    @Test
    public void testExecutorErrorFailsTheInvocationAndReleasesTheInvoker() throws InterruptedException {
        final ThreadPoolExecutor pool = newPool(1);
        final AtomicInteger submissions = new AtomicInteger();
        final OutOfMemoryError cannotMakeThread = new OutOfMemoryError("unable to create native thread");
        final Executor failsSecondSubmission = command -> {
            if (submissions.incrementAndGet() > 1) {
                throw cannotMakeThread;
            }
            pool.execute(command);
        };
        final AtomicInteger started = new AtomicInteger();
        final AtomicInteger finished = new AtomicInteger();
        final AtomicInteger contextsOpen = new AtomicInteger();
        final Supplier<JobScheduler.JobThreadContext> countingContexts = () -> {
            contextsOpen.incrementAndGet();
            return new JobScheduler.JobThreadContext() {
                @Override
                public void close() {
                    contextsOpen.decrementAndGet();
                }
            };
        };

        withTimeout(() -> {
            assertThatThrownBy(() -> new ExecutorJobScheduler(failsSecondSubmission, 4).invokeParallel(
                    ExecutionContext.getContext(), null, countingContexts, 0, 8,
                    (context, idx, nec) -> {
                        started.incrementAndGet();
                        sleep(100);
                        finished.incrementAndGet();
                    })).isSameAs(cannotMakeThread);
        });

        assertThat(started.get()).isLessThanOrEqualTo(1);
        assertThat(finished.get()).isEqualTo(started.get());
        assertThat(contextsOpen.get()).isZero();
    }

    /** An Error from the context factory fails the invocation before any task runs, and is thrown. */
    @Test
    public void testContextFactoryErrorIsThrown() throws InterruptedException {
        final AssertionError factoryError = new AssertionError("no context");
        final AtomicInteger runs = new AtomicInteger();

        withTimeout(() -> {
            assertThatThrownBy(() -> newScheduler(3, 4).invokeParallel(ExecutionContext.getContext(), null,
                    () -> {
                        throw factoryError;
                    }, 0, 10, (context, idx, nec) -> runs.incrementAndGet())).isSameAs(factoryError);
        });

        assertThat(runs.get()).isZero();
    }

    /**
     * A single-thread executor's own worker may invoke on a scheduler over that executor: the helper it submits is
     * queued behind the worker itself, and the worker must finish the iteration without waiting for it.
     */
    @Test
    public void testInvokeFromTheOnlyThreadOfAQueuedExecutorCompletes() throws Exception {
        final ExecutorService single = Executors.newSingleThreadExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "invoke-parallel-test-single");
            thread.setDaemon(true);
            return thread;
        });
        try {
            final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(single, 4);
            final ExecutionContext executionContext = ExecutionContext.getContext();
            final AtomicIntegerArray runs = new AtomicIntegerArray(20);
            final Future<?> done = single.submit(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    invoke(scheduler, 20, (context, idx, nec) -> runs.incrementAndGet(idx));
                }
            });
            done.get(30, TimeUnit.SECONDS);
            for (int ii = 0; ii < 20; ++ii) {
                assertThat(runs.get(ii)).isEqualTo(1);
            }
        } finally {
            single.shutdownNow();
        }
    }

    /**
     * A queueing executor may start a helper only after the caller has run every task; the caller must not wait for it,
     * and the helper, when it does start, must find nothing to do and leave no context open.
     */
    @Test
    public void testDoesNotWaitForHelpersThatHaveNotStarted() {
        final List<Runnable> queued = Collections.synchronizedList(new ArrayList<>());
        final Executor deferring = queued::add;
        final AtomicInteger contextsOpen = new AtomicInteger();
        final AtomicInteger runs = new AtomicInteger();
        final Set<Thread> threads = ConcurrentHashMap.newKeySet();

        invoke(new ExecutorJobScheduler(deferring, 4),
                () -> {
                    contextsOpen.incrementAndGet();
                    return new JobScheduler.JobThreadContext() {
                        @Override
                        public void close() {
                            contextsOpen.decrementAndGet();
                        }
                    };
                }, 10, (context, idx, nec) -> {
                    runs.incrementAndGet();
                    threads.add(Thread.currentThread());
                });
        assertThat(runs.get()).isEqualTo(10);
        assertThat(threads).containsExactly(Thread.currentThread());
        assertThat(queued).hasSize(3);
        assertThat(contextsOpen.get()).isZero();

        // the late jobs find their invokers already run by the caller, and do nothing
        queued.forEach(Runnable::run);
        assertThat(runs.get()).isEqualTo(10);
    }

    /**
     * When a later submission fails, invokers submitted before it to an executor that queues may never start; the
     * caller must close them and fail rather than wait for them.
     */
    @Test
    public void testLaterSubmissionFailureClosesQueuedInvokers() throws InterruptedException {
        final List<Runnable> queued = Collections.synchronizedList(new ArrayList<>());
        final IllegalStateException broken = new IllegalStateException("cannot start a thread");
        final Executor queuesFirstFailsSecond = command -> {
            if (!queued.isEmpty()) {
                throw broken;
            }
            queued.add(command);
        };
        final AtomicInteger contextsOpen = new AtomicInteger();
        final AtomicInteger runs = new AtomicInteger();

        withTimeout(() -> {
            assertThatThrownBy(() -> new ExecutorJobScheduler(queuesFirstFailsSecond, 4).invokeParallel(
                    ExecutionContext.getContext(), null,
                    () -> {
                        contextsOpen.incrementAndGet();
                        return new JobScheduler.JobThreadContext() {
                            @Override
                            public void close() {
                                contextsOpen.decrementAndGet();
                            }
                        };
                    }, 0, 8, (context, idx, nec) -> runs.incrementAndGet())).isSameAs(broken);
        });

        assertThat(contextsOpen.get()).isZero();
        assertThat(runs.get()).isZero();
        // the queued job, started late, finds its invoker taken
        queued.forEach(Runnable::run);
        assertThat(runs.get()).isZero();
    }

    /** The same when the context factory fails for a later invoker. */
    @Test
    public void testLaterContextFailureClosesQueuedInvokers() throws InterruptedException {
        final List<Runnable> queued = Collections.synchronizedList(new ArrayList<>());
        final IllegalStateException noContext = new IllegalStateException("no context");
        final AtomicInteger made = new AtomicInteger();
        final AtomicInteger contextsOpen = new AtomicInteger();
        final AtomicInteger runs = new AtomicInteger();

        withTimeout(() -> {
            assertThatThrownBy(() -> new ExecutorJobScheduler(queued::add, 4).invokeParallel(
                    ExecutionContext.getContext(), null,
                    () -> {
                        // the caller's own context, then the first helper's; the second helper's fails
                        if (made.incrementAndGet() > 2) {
                            throw noContext;
                        }
                        contextsOpen.incrementAndGet();
                        return new JobScheduler.JobThreadContext() {
                            @Override
                            public void close() {
                                contextsOpen.decrementAndGet();
                            }
                        };
                    }, 0, 8, (context, idx, nec) -> runs.incrementAndGet())).isSameAs(noContext);
        });

        assertThat(contextsOpen.get()).isZero();
        assertThat(queued).hasSize(1);
        queued.forEach(Runnable::run);
        assertThat(runs.get()).isZero();
    }

    /**
     * A context that fails to close while the unstarted invokers are abandoned must not stop the others closing: the
     * queued helper's context is still closed and its reference released, and the close failure rides on the thrown
     * failure.
     */
    @Test
    public void testContextCloseFailureStillClosesTheOtherUnstartedInvokers() throws InterruptedException {
        final List<Runnable> queued = Collections.synchronizedList(new ArrayList<>());
        final IllegalStateException noContext = new IllegalStateException("no context");
        final IllegalArgumentException closeFailure = new IllegalArgumentException("close failed");
        final AtomicInteger made = new AtomicInteger();
        final AtomicInteger contextsOpen = new AtomicInteger();
        final AtomicInteger runs = new AtomicInteger();

        withTimeout(() -> {
            assertThatThrownBy(() -> new ExecutorJobScheduler(queued::add, 4).invokeParallel(
                    ExecutionContext.getContext(), null,
                    () -> {
                        // the caller's own context, which fails to close, then the first helper's; the second fails
                        final int index = made.incrementAndGet();
                        if (index > 2) {
                            throw noContext;
                        }
                        contextsOpen.incrementAndGet();
                        return new JobScheduler.JobThreadContext() {
                            @Override
                            public void close() {
                                contextsOpen.decrementAndGet();
                                if (index == 1) {
                                    throw closeFailure;
                                }
                            }
                        };
                    }, 0, 8, (context, idx, nec) -> runs.incrementAndGet()))
                    .isSameAs(noContext)
                    .satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(closeFailure));
        });

        assertThat(contextsOpen.get()).isZero();
        assertThat(queued).hasSize(1);
        queued.forEach(Runnable::run);
        assertThat(runs.get()).isZero();
    }

    @Test
    public void testFailureOnTheCallingThreadStopsAtTheFailingTask() {
        final List<Integer> ran = new ArrayList<>();
        final IllegalStateException failure = new IllegalStateException("boom");

        assertThatThrownBy(() -> new ImmediateJobScheduler().invokeParallel(ExecutionContext.getContext(), null,
                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 5,
                (context, idx, nec) -> {
                    ran.add(idx);
                    if (idx == 2) {
                        throw failure;
                    }
                })).isSameAs(failure);

        assertThat(ran).containsExactly(0, 1, 2);
    }

    /** A checked exception reaching onError, as nested work may deliver one, is thrown wrapped. */
    @Test
    public void testCheckedFailureIsThrownWrapped() {
        final Exception checked = new Exception("checked");

        assertThatThrownBy(() -> new ImmediateJobScheduler().invokeParallel(ExecutionContext.getContext(), null,
                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 2,
                (context, idx, nestedErrorConsumer, resume) -> nestedErrorConsumer.accept(checked)))
                .isInstanceOf(UncheckedDeephavenException.class)
                .hasCause(checked);
    }

    /** A failure to start the iteration fails it like any other failure, and is thrown. */
    @Test
    public void testStartFailureIsThrown() {
        final IllegalStateException factoryFailure = new IllegalStateException("no context");
        final AtomicInteger runs = new AtomicInteger();

        assertThatThrownBy(() -> newScheduler(3, 4).invokeParallel(ExecutionContext.getContext(), null,
                () -> {
                    throw factoryFailure;
                }, 0, 10, (context, idx, nec) -> runs.incrementAndGet())).isSameAs(factoryFailure);

        assertThat(runs.get()).isZero();
    }

    /**
     * The callback form shares the iteration's start: a failure to start it, which it throws to its caller, also ends
     * it in onError, never in onComplete.
     */
    @Test
    public void testCallbackFormStartFailureEndsInOnError() {
        final IllegalStateException factoryFailure = new IllegalStateException("no context");
        final Callbacks callbacks = new Callbacks();

        assertThatThrownBy(() -> newScheduler(3, 4).iterateParallel(ExecutionContext.getContext(), null,
                () -> {
                    throw factoryFailure;
                }, 0, 10, (context, idx, nec) -> {
                },
                callbacks.onComplete, callbacks.cleanup, callbacks.onError)).isSameAs(factoryFailure);

        assertThat(callbacks.completeCalls.get()).isZero();
        assertThat(callbacks.cleanupCalls.get()).isZero();
        assertThat(callbacks.error.get()).isSameAs(factoryFailure);
    }

    /** The same for an Error from the context factory, which reaches onError wrapped. */
    @Test
    public void testCallbackFormStartErrorEndsInOnError() {
        final AssertionError factoryError = new AssertionError("no context");
        final Callbacks callbacks = new Callbacks();

        assertThatThrownBy(() -> newScheduler(3, 4).iterateParallel(ExecutionContext.getContext(), null,
                () -> {
                    throw factoryError;
                }, 0, 10, (context, idx, nec) -> {
                },
                callbacks.onComplete, callbacks.cleanup, callbacks.onError)).isSameAs(factoryError);

        assertThat(callbacks.completeCalls.get()).isZero();
        assertThat(callbacks.cleanupCalls.get()).isZero();
        assertThat(callbacks.error.get()).isInstanceOf(UncheckedDeephavenException.class);
        assertThat(callbacks.error.get().getCause()).isSameAs(factoryError);
    }

    @Test
    public void testUpdateGraphSchedulerRefusesToBlock() {
        final JobScheduler scheduler =
                new UpdateGraphJobScheduler(ExecutionContext.getContext().getUpdateGraph());
        final AtomicInteger runs = new AtomicInteger();
        final AtomicInteger contexts = new AtomicInteger();

        assertThatThrownBy(() -> scheduler.invokeParallel(ExecutionContext.getContext(), null,
                () -> {
                    contexts.incrementAndGet();
                    return JobScheduler.DEFAULT_CONTEXT;
                }, 0, 10, (context, idx, nec) -> runs.incrementAndGet()))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThat(runs.get()).isZero();
        assertThat(contexts.get()).isZero();
    }

    @Test
    public void testOperationInitializerSchedulerRunsTheIteration() {
        final AtomicIntegerArray runs = new AtomicIntegerArray(50);
        invoke(new OperationInitializerJobScheduler(), 50,
                (context, idx, nec) -> runs.incrementAndGet(idx));

        for (int ii = 0; ii < 50; ++ii) {
            assertThat(runs.get(ii)).isEqualTo(1);
        }
    }

    /**
     * An operation initializer that fails a submission with an Error leaves no job counted as outstanding, so the
     * performance the caller collects afterwards is available rather than waited for forever.
     */
    @Test
    public void testOperationInitializerSubmitErrorReleasesTheOutstandingCount() throws InterruptedException {
        final OutOfMemoryError cannotMakeThread = new OutOfMemoryError("unable to create native thread");
        final OperationInitializer failing = new OperationInitializer() {
            @Override
            public boolean canParallelize() {
                return true;
            }

            @Override
            public Future<?> submit(final Runnable runnable) {
                throw cannotMakeThread;
            }

            @Override
            public int parallelismFactor() {
                return 4;
            }
        };
        final OperationInitializerJobScheduler scheduler = new OperationInitializerJobScheduler(failing);
        final AtomicInteger runs = new AtomicInteger();

        withTimeout(() -> {
            assertThatThrownBy(() -> scheduler.invokeParallel(ExecutionContext.getContext(), null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 8, (context, idx, nec) -> runs.incrementAndGet()))
                    .isSameAs(cannotMakeThread);
            assertThat(scheduler.getAccumulatedPerformance()).isNotNull();
        });
        assertThat(runs.get()).isZero();
    }

    /**
     * An inline operation initializer runs a job inside submit; when that job ends in an Error, the count it took is
     * released once, in the job, and not again by submit, so that later waits for outstanding work still balance.
     */
    @Test
    public void testInlineOperationInitializerErrorReleasesTheCountOnce() throws InterruptedException {
        final AssertionError taskError = new AssertionError("task error");
        final OperationInitializerJobScheduler scheduler =
                new OperationInitializerJobScheduler(OperationInitializer.NON_PARALLELIZABLE);
        // the scheduler reports the task's Error as fatal and rethrows it; the unit test reporter throws in its place
        final Throwable thrown = catchThrowable(() -> scheduler.submit(null, () -> {
            throw taskError;
        }, null, e -> {
        }));
        assertThat(thrown).isNotNull();

        // A second, successful job: if the first had released its count twice, the count would now sit below zero
        // and this job's release would leave it at zero early, or the wait below would never see it reach zero.
        final AtomicInteger runs = new AtomicInteger();
        scheduler.submit(null, runs::incrementAndGet, null, e -> {
        });
        assertThat(runs.get()).isEqualTo(1);
        withTimeout(() -> assertThat(scheduler.getAccumulatedPerformance()).isNotNull());
        final AtomicInteger outstanding = outstandingJobs(scheduler);
        assertThat(outstanding.get()).isZero();
    }

    /** The scheduler's outstanding job count, which nothing else exposes. */
    private static AtomicInteger outstandingJobs(final OperationInitializerJobScheduler scheduler) {
        try {
            final java.lang.reflect.Field field =
                    OperationInitializerJobScheduler.class.getDeclaredField("outstandingJobs");
            field.setAccessible(true);
            return (AtomicInteger) field.get(scheduler);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** One context per invoker, never shared between threads at once, and all closed before the invocation returns. */
    @Test
    public void testOneContextPerInvokerClosedBeforeReturning() {
        final int threadCount = 4;
        final ExecutorJobScheduler scheduler = newScheduler(threadCount - 1, threadCount);

        final class CountingContext implements JobScheduler.JobThreadContext {
            final AtomicInteger inUse = new AtomicInteger();
            final AtomicInteger tasks = new AtomicInteger();
            volatile boolean closed;

            @Override
            public void close() {
                closed = true;
            }
        }
        final List<CountingContext> contexts = Collections.synchronizedList(new ArrayList<>());
        final AtomicBoolean sharedAtOnce = new AtomicBoolean();

        scheduler.invokeParallel(ExecutionContext.getContext(), null,
                () -> {
                    final CountingContext context = new CountingContext();
                    contexts.add(context);
                    return context;
                }, 0, 100,
                (context, idx, nec) -> {
                    if (context.inUse.incrementAndGet() != 1) {
                        sharedAtOnce.set(true);
                    }
                    context.tasks.incrementAndGet();
                    sleep(1);
                    context.inUse.decrementAndGet();
                });

        assertThat(contexts).hasSizeBetween(1, threadCount);
        assertThat(sharedAtOnce.get()).as("a context was in use by two tasks at once").isFalse();
        assertThat(contexts).as("contexts still open when the invocation returned").allMatch(context -> context.closed);
        assertThat(contexts.stream().mapToInt(context -> context.tasks.get()).sum()).isEqualTo(100);
    }

    @Test
    public void testInvokeSerialRunsStepsInOrderOnTheCallingThread() {
        final ExecutorJobScheduler scheduler = newScheduler(3, 4);
        final List<Integer> order = new ArrayList<>();
        final Set<Thread> threads = ConcurrentHashMap.newKeySet();
        final AtomicInteger contexts = new AtomicInteger();

        scheduler.invokeSerial(ExecutionContext.getContext(), null,
                () -> {
                    contexts.incrementAndGet();
                    return JobScheduler.DEFAULT_CONTEXT;
                }, 0, 10,
                (context, idx, nestedErrorConsumer, resume) -> {
                    order.add(idx);
                    threads.add(Thread.currentThread());
                    resume.run();
                });

        assertThat(order).isEqualTo(items(10));
        assertThat(threads).containsExactly(Thread.currentThread());
        assertThat(contexts.get()).isEqualTo(1);
    }

    /**
     * Each step fans out beneath itself and completes through resume; the steps still run one at a time and in order,
     * whichever thread finishes a step's nested work runs the next, and the caller waits for the whole chain.
     */
    @Test
    public void testInvokeSerialStepsWithNestedParallelWorkRunOneAtATime() throws InterruptedException {
        final ExecutorJobScheduler scheduler = newScheduler(3, 4);
        final List<Integer> order = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger activeSteps = new AtomicInteger();
        final AtomicBoolean stepsOverlapped = new AtomicBoolean();
        final AtomicInteger innerRuns = new AtomicInteger();

        withTimeout(() -> {
            scheduler.invokeSerial(ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0, 6,
                    (context, step, nestedErrorConsumer, resume) -> {
                        if (activeSteps.incrementAndGet() != 1) {
                            stepsOverlapped.set(true);
                        }
                        order.add(step);
                        scheduler.iterateParallel(ExecutionContext.getContext(), null,
                                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 4,
                                (innerContext, inner, innerNec) -> {
                                    sleep(2);
                                    innerRuns.incrementAndGet();
                                },
                                () -> {
                                    activeSteps.decrementAndGet();
                                    resume.run();
                                }, () -> {
                                }, nestedErrorConsumer);
                    });
        });

        assertThat(order).isEqualTo(items(6));
        assertThat(stepsOverlapped.get()).as("two steps were active at once").isFalse();
        assertThat(innerRuns.get()).isEqualTo(24);
    }

    @Test
    public void testInvokeSerialFailureStopsTheChainAndIsThrown() throws InterruptedException {
        final ExecutorJobScheduler scheduler = newScheduler(1, 2);
        final IllegalStateException failure = new IllegalStateException("step failed");
        final List<Integer> order = Collections.synchronizedList(new ArrayList<>());

        withTimeout(() -> {
            assertThatThrownBy(() -> scheduler.invokeSerial(ExecutionContext.getContext(), null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 6,
                    (context, step, nestedErrorConsumer, resume) -> {
                        order.add(step);
                        // the nested work of step 2 fails, and reports it through the nested error consumer
                        scheduler.iterateParallel(ExecutionContext.getContext(), null,
                                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 3,
                                (innerContext, inner, innerNec) -> {
                                    if (step == 2 && inner == 1) {
                                        throw failure;
                                    }
                                },
                                resume, () -> {
                                }, nestedErrorConsumer);
                    })).isSameAs(failure);
        });

        assertThat(order).isEqualTo(items(3));
    }

    @Test
    public void testInvokeSerialOnTheUpdateGraphSchedulerIsRefused() {
        final JobScheduler scheduler =
                new UpdateGraphJobScheduler(ExecutionContext.getContext().getUpdateGraph());
        final AtomicInteger runs = new AtomicInteger();

        assertThatThrownBy(() -> scheduler.invokeSerial(ExecutionContext.getContext(), null,
                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 10,
                (context, idx, nestedErrorConsumer, resume) -> {
                    runs.incrementAndGet();
                    resume.run();
                })).isInstanceOf(UnsupportedOperationException.class);

        assertThat(runs.get()).isZero();
    }

    private static Throwable catchThrowable(final Runnable runnable) {
        try {
            runnable.run();
        } catch (final Throwable t) {
            return t;
        }
        throw new AssertionError("expected a throwable");
    }
}
