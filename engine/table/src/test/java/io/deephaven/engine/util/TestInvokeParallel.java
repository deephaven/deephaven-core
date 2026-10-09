//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.util.ExecutorJobScheduler;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler;
import io.deephaven.engine.table.impl.util.UpdateGraphJobScheduler;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.testcase.FakeProcessEnvironment;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
 * Tests of {@link JobScheduler#invokeParallel}: the calling thread takes part, the call returns only once the iteration
 * is over, and a failure is thrown to the caller.
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

    private static void await(final CountDownLatch latch) {
        try {
            assertThat(latch.await(30, TimeUnit.SECONDS)).as("latch released in time").isTrue();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fails if a fatal report reached {@code thrown}. The unit test reporter throws in place of ending the process, so
     * a report made on the calling thread surfaces among the causes and suppressed exceptions of what is thrown.
     */
    private static void assertNoFatalReport(final Throwable thrown) {
        final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        final Deque<Throwable> pending = new ArrayDeque<>();
        pending.push(thrown);
        while (!pending.isEmpty()) {
            final Throwable next = pending.pop();
            if (!seen.add(next)) {
                continue;
            }
            assertThat(next).as("a fatal report, in %s", thrown)
                    .isNotInstanceOf(FakeProcessEnvironment.FakeFatalException.class);
            if (next.getCause() != null) {
                pending.push(next.getCause());
            }
            for (final Throwable suppressed : next.getSuppressed()) {
                pending.push(suppressed);
            }
        }
    }

    /**
     * Runs each job through {@code executor}, recording whatever escapes it, as a fatal report made on a scheduler
     * thread does.
     */
    private static Executor recordingEscapes(final Executor executor, final List<Throwable> escaped) {
        return command -> executor.execute(() -> {
            try {
                command.run();
            } catch (final Throwable t) {
                escaped.add(t);
            }
        });
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
        final List<Integer> order = Collections.synchronizedList(new ArrayList<>());
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

    /**
     * A helper whose context fails to close after every task has completed fails the invocation, rather than letting it
     * return while the failure goes only to the global reporter.
     */
    @Test
    public void testHelperContextCloseFailureFailsTheInvocation() {
        final int threadCount = 4;
        final ExecutorJobScheduler scheduler = newScheduler(threadCount - 1, threadCount);
        final CyclicBarrier rendezvous = new CyclicBarrier(threadCount);
        final Thread caller = Thread.currentThread();
        final IllegalStateException closeFailure = new IllegalStateException("close failed");
        final AtomicInteger runs = new AtomicInteger();

        assertThatThrownBy(() -> scheduler.invokeParallel(ExecutionContext.getContext(), null,
                () -> new JobScheduler.JobThreadContext() {
                    @Override
                    public void close() {
                        if (Thread.currentThread() != caller) {
                            throw closeFailure;
                        }
                    }
                }, 0, threadCount,
                (context, idx, nec) -> {
                    // one task on each thread, so that every helper closes its own context
                    await(rendezvous);
                    runs.incrementAndGet();
                })).isSameAs(closeFailure);

        assertThat(runs.get()).isEqualTo(threadCount);
    }

    /**
     * An Error on the calling thread after another task has already failed is kept, attached to the failure that is
     * thrown, rather than dropped because some failure was already recorded.
     */
    @Test
    public void testLaterErrorOnTheCallingThreadIsSuppressedOnTheFirstFailure() {
        final IllegalStateException taskFailure = new IllegalStateException("task failed");
        final AssertionError closeError = new AssertionError("close error");
        final AtomicInteger made = new AtomicInteger();
        // runs the helper inline, so that its task fails before the caller runs its own invoker
        final Executor inline = Runnable::run;

        assertThatThrownBy(() -> new ExecutorJobScheduler(inline, 4).invokeParallel(ExecutionContext.getContext(),
                null,
                () -> {
                    final boolean callersOwn = made.incrementAndGet() == 1;
                    return new JobScheduler.JobThreadContext() {
                        @Override
                        public void close() {
                            if (callersOwn) {
                                throw closeError;
                            }
                        }
                    };
                }, 0, 2,
                (context, idx, nec) -> {
                    if (idx == 1) {
                        throw taskFailure;
                    }
                }))
                .isSameAs(taskFailure)
                .satisfies(thrown -> assertThat(thrown.getSuppressed()).hasSize(1)
                        .allSatisfy(suppressed -> assertThat(suppressed.getCause()).isSameAs(closeError)));
    }

    /**
     * A submission that runs its job inline and then throws does not fail the iteration; its exception is the caller's,
     * thrown once the iteration is over. The caller's own invoker, which nothing has started, is closed rather than
     * waited on forever.
     */
    @Test
    public void testSubmitThatThrowsAfterRunningInlineIsThrownWithoutHanging() {
        final IllegalStateException afterRunning = new IllegalStateException("thrown after running the job");
        final Executor runsThenThrows = command -> {
            command.run();
            throw afterRunning;
        };

        assertThatThrownBy(() -> withTimeout(() -> invoke(new ExecutorJobScheduler(runsThenThrows, 2), 2,
                (context, idx, nec) -> {
                })))
                .isSameAs(afterRunning)
                .satisfies(TestInvokeParallel::assertNoFatalReport);
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
     * The callback form shares the iteration's start: a failure to start it ends it in onError, never in onComplete,
     * and reaches the caller only that way.
     */
    @Test
    public void testCallbackFormStartFailureEndsInOnError() {
        final IllegalStateException factoryFailure = new IllegalStateException("no context");
        final Callbacks callbacks = new Callbacks();

        newScheduler(3, 4).iterateParallel(ExecutionContext.getContext(), null,
                () -> {
                    throw factoryFailure;
                }, 0, 10, (context, idx, nec) -> {
                },
                callbacks.onComplete, callbacks.cleanup, callbacks.onError);

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

    /**
     * A step that hands resume and its nested error consumer to a nested iteration that fails to start ends the
     * iteration in onError once, with no fatal report on the scheduler thread running the step.
     */
    @Test
    public void testNestedStartFailureEndsTheCallbackFormInOnErrorWithoutAFatalReport() throws InterruptedException {
        final ThreadPoolExecutor pool = newPool(2);
        final List<Throwable> escaped = Collections.synchronizedList(new ArrayList<>());
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(recordingEscapes(pool, escaped), 3);
        final IllegalStateException noContext = new IllegalStateException("nested context factory failed");
        final Callbacks callbacks = new Callbacks();
        final CountDownLatch ended = new CountDownLatch(1);

        scheduler.iterateSerial(ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 3,
                (context, step, nestedErrorConsumer, resume) -> scheduler.iterateParallel(
                        ExecutionContext.getContext(), null,
                        () -> {
                            throw noContext;
                        }, 0, 4, (innerContext, inner, innerNec) -> {
                        }, resume, () -> {
                        }, nestedErrorConsumer),
                callbacks.onComplete, callbacks.cleanup, e -> {
                    callbacks.onError.accept(e);
                    ended.countDown();
                });
        await(ended);
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        assertThat(callbacks.error.get()).isSameAs(noContext);
        assertThat(callbacks.completeCalls.get()).isZero();
        assertThat(callbacks.cleanupCalls.get()).isZero();
        assertThat(escaped).isEmpty();
    }

    /**
     * A nested iteration whose later submission is refused while one of its jobs is still running ends in onError once
     * that job finishes, and through the step's nested error consumer ends the outer iteration in onError once, with no
     * fatal report on any thread.
     */
    @Test
    public void testNestedSubmitRefusalEndsTheOuterIterationInOnErrorOnce() {
        final ThreadPoolExecutor pool = newPool(2);
        final List<Throwable> escaped = Collections.synchronizedList(new ArrayList<>());
        final IllegalStateException submitFailure = new IllegalStateException("cannot start a thread");
        final CountDownLatch refused = new CountDownLatch(1);
        final CountDownLatch ended = new CountDownLatch(1);
        final AtomicInteger submissions = new AtomicInteger();
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(recordingEscapes(command -> {
            // the outer step and the first nested job start; the second nested job's submission is refused
            if (submissions.incrementAndGet() > 2) {
                refused.countDown();
                throw submitFailure;
            }
            pool.execute(command);
        }, escaped), 3);
        final Callbacks callbacks = new Callbacks();

        scheduler.iterateSerial(ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 1,
                (context, step, nestedErrorConsumer, resume) -> scheduler.iterateParallel(
                        ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 4,
                        // the first nested job outlasts the refusal
                        (innerContext, inner, innerNec) -> await(refused),
                        resume, () -> {
                        }, nestedErrorConsumer),
                callbacks.onComplete, callbacks.cleanup, e -> {
                    callbacks.onError.accept(e);
                    ended.countDown();
                });
        await(ended);
        pool.shutdown();
        try {
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertThat(callbacks.error.get()).isSameAs(submitFailure);
        assertThat(callbacks.completeCalls.get()).isZero();
        assertThat(callbacks.cleanupCalls.get()).isZero();
        assertThat(escaped).isEmpty();
    }

    /**
     * A task that reports its failure through the nested error consumer and then throws it fails the invocation once.
     */
    @Test
    public void testFailureReportedThenThrownIsNotFatal() {
        final IllegalStateException failure = new IllegalStateException("reported, then thrown");

        assertThatThrownBy(() -> new ImmediateJobScheduler().invokeParallel(ExecutionContext.getContext(), null,
                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 3,
                (context, idx, nestedErrorConsumer) -> {
                    nestedErrorConsumer.accept(failure);
                    throw failure;
                }))
                .isSameAs(failure)
                .satisfies(TestInvokeParallel::assertNoFatalReport)
                .satisfies(thrown -> assertThat(thrown.getSuppressed()).isEmpty());
    }

    /**
     * A context that fails to close with an Error on the calling thread, after every task succeeded, fails the
     * invocation with that Error, thrown as itself like any Error on the calling thread, not as a fatal report.
     */
    @Test
    public void testCallerContextCloseErrorAfterSuccessIsThrownAsItself() {
        final AssertionError closeError = new AssertionError("close error");
        final AtomicInteger runs = new AtomicInteger();

        assertThatThrownBy(() -> new ImmediateJobScheduler().invokeParallel(ExecutionContext.getContext(), null,
                () -> new JobScheduler.JobThreadContext() {
                    @Override
                    public void close() {
                        throw closeError;
                    }
                }, 0, 3, (context, idx, nec) -> runs.incrementAndGet()))
                .isSameAs(closeError)
                .satisfies(TestInvokeParallel::assertNoFatalReport);

        assertThat(runs.get()).isEqualTo(3);
    }

    /**
     * An Error on the calling thread is thrown as itself once the helpers have finished, carrying the failures they
     * reported after it.
     */
    @Test
    public void testErrorOnTheCallingThreadCarriesLaterFailures() {
        final ExecutorJobScheduler scheduler = newScheduler(1, 2);
        final Thread caller = Thread.currentThread();
        final AssertionError error = new AssertionError("caller's task error");
        final IllegalStateException later = new IllegalStateException("helper's later failure");
        final CountDownLatch helperStarted = new CountDownLatch(1);
        final CountDownLatch errorRecorded = new CountDownLatch(1);
        final AtomicBoolean helperDone = new AtomicBoolean();
        final AtomicInteger made = new AtomicInteger();

        assertThatThrownBy(() -> scheduler.invokeParallel(ExecutionContext.getContext(), null,
                () -> {
                    // the caller's own context is made first, and closed once its task's Error is recorded
                    final boolean callersOwn = made.incrementAndGet() == 1;
                    return new JobScheduler.JobThreadContext() {
                        @Override
                        public void close() {
                            if (callersOwn) {
                                errorRecorded.countDown();
                            }
                        }
                    };
                }, 0, 2,
                (context, idx, nec) -> {
                    if (Thread.currentThread() == caller) {
                        await(helperStarted);
                        throw error;
                    }
                    helperStarted.countDown();
                    await(errorRecorded);
                    helperDone.set(true);
                    throw later;
                }))
                .isSameAs(error)
                .satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(later));

        assertThat(helperDone.get()).as("the helper's task had finished when the Error was thrown").isTrue();
    }

    /**
     * A submission that fails after a helper has already failed does not displace that first failure, whether or not
     * another helper is still running when it fails: the helper's failure is thrown, carrying the submission's failure
     * once, and nothing is suppressed on the submission's failure in turn.
     */
    @Test
    public void testStartFailureAfterAHelperFailedIsSuppressedOnTheFirstFailure() throws InterruptedException {
        final ThreadPoolExecutor pool = newPool(2);
        final IllegalStateException helperFailure = new IllegalStateException("helper's task failed");
        final IllegalArgumentException submitFailure = new IllegalArgumentException("cannot start a thread");
        final CountDownLatch thirdSubmitting = new CountDownLatch(1);
        final CountDownLatch helperFailed = new CountDownLatch(1);
        final CountDownLatch callersContextClosed = new CountDownLatch(1);
        final AtomicInteger submissions = new AtomicInteger();
        final Executor failsThirdSubmission = command -> {
            if (submissions.incrementAndGet() == 3) {
                // fail once the first helper's failure has been recorded
                thirdSubmitting.countDown();
                await(helperFailed);
                throw submitFailure;
            }
            pool.execute(command);
        };
        final AtomicInteger made = new AtomicInteger();
        final AtomicInteger contextsOpen = new AtomicInteger();
        final Supplier<JobScheduler.JobThreadContext> contexts = () -> {
            // the caller's own, then those of the first, second and third helpers
            final int index = made.incrementAndGet();
            contextsOpen.incrementAndGet();
            return new JobScheduler.JobThreadContext() {
                @Override
                public void close() {
                    contextsOpen.decrementAndGet();
                    if (index == 1) {
                        callersContextClosed.countDown();
                    } else if (index == 2) {
                        helperFailed.countDown();
                    }
                }
            };
        };

        withTimeout(() -> assertThatThrownBy(() -> new ExecutorJobScheduler(failsThirdSubmission, 4).invokeParallel(
                ExecutionContext.getContext(), null, contexts, 0, 8,
                (context, idx, nec) -> {
                    if (idx == 1) {
                        // fail only once every helper has been made, so that the third submission is attempted
                        await(thirdSubmitting);
                        throw helperFailure;
                    }
                    if (idx == 2) {
                        // run on until the caller, failing to start, abandons its own unstarted invoker
                        await(callersContextClosed);
                    }
                }))
                .isSameAs(helperFailure)
                .satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(submitFailure))
                .satisfies(thrown -> assertThat(submitFailure.getSuppressed()).isEmpty()));

        assertThat(contextsOpen.get()).isZero();
    }

    /**
     * An onError that rethrows the failure it was given is reported as fatal with that failure, rather than with the
     * IllegalArgumentException that suppressing it on itself would throw.
     */
    @Test
    public void testOnErrorRethrowingItsFailureReportsThatFailure() {
        final IllegalStateException failure = new IllegalStateException("task failed");

        assertThatThrownBy(() -> new ImmediateJobScheduler().iterateParallel(ExecutionContext.getContext(), null,
                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 2,
                (context, idx, nec) -> {
                    throw failure;
                },
                () -> {
                }, () -> {
                }, e -> {
                    throw (RuntimeException) e;
                }))
                .isInstanceOf(FakeProcessEnvironment.FakeFatalException.class)
                .satisfies(thrown -> assertThat(thrown.getCause()).isSameAs(failure));
    }

    /**
     * A task of an immediate scheduler's iteration, which runs as one of that scheduler's jobs, may invoke on the same
     * scheduler: the invocation submits nothing there and runs every task on that thread. It may invoke on a scheduler
     * of its own too.
     */
    @Test
    public void testInvokeFromAnImmediateSchedulersOwnTaskCompletes() throws InterruptedException {
        final ImmediateJobScheduler scheduler = new ImmediateJobScheduler();
        final AtomicInteger sameSchedulerRuns = new AtomicInteger();
        final AtomicInteger ownRuns = new AtomicInteger();
        final Callbacks callbacks = new Callbacks();

        withTimeout(() -> scheduler.iterateParallel(ExecutionContext.getContext(), null,
                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 2,
                (context, idx, nec) -> {
                    invoke(scheduler, 4, (innerContext, inner, innerNec) -> sameSchedulerRuns.incrementAndGet());
                    invoke(new ImmediateJobScheduler(), 3, (ownContext, own, ownNec) -> ownRuns.incrementAndGet());
                },
                callbacks.onComplete, callbacks.cleanup, callbacks.onError));

        assertThat(callbacks.error.get()).isNull();
        assertThat(callbacks.completeCalls.get()).isEqualTo(1);
        assertThat(sameSchedulerRuns.get()).isEqualTo(8);
        assertThat(ownRuns.get()).isEqualTo(6);
    }

    /** The test's update graph, reset to deliver notifications on {@code updateThreads} threads of its own. */
    private static ControlledUpdateGraph updateGraphWithUpdateThreads(final int updateThreads) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, updateThreads, 0, 0);
        return updateGraph;
    }

    /** Supplies task contexts that count how many were made and how many are still open. */
    private static final class CountingContexts implements Supplier<JobScheduler.JobThreadContext> {
        private final AtomicInteger made = new AtomicInteger();
        private final AtomicInteger open = new AtomicInteger();

        @Override
        public JobScheduler.JobThreadContext get() {
            made.incrementAndGet();
            open.incrementAndGet();
            return new JobScheduler.JobThreadContext() {
                @Override
                public void close() {
                    open.decrementAndGet();
                }
            };
        }
    }

    private static void assertEachRanOnce(final AtomicIntegerArray runs) {
        for (int ii = 0; ii < runs.length(); ++ii) {
            assertThat(runs.get(ii)).as("runs of task %d", ii).isEqualTo(1);
        }
    }

    /**
     * An invocation may block an update thread. On a pool thread, the refresh thread dispatches its helper to another
     * pool thread, which works alongside it: the two tasks meet at a barrier, which they can pass only on two threads
     * at once. Invocations nested in those tasks complete too.
     */
    @Test
    public void testInvokeOnAPoolThreadIsHelpedByAnotherPoolThread() {
        final ControlledUpdateGraph updateGraph = updateGraphWithUpdateThreads(4);
        final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
        final CyclicBarrier bothRunning = new CyclicBarrier(2);
        final Set<Thread> taskThreads = ConcurrentHashMap.newKeySet();
        final AtomicBoolean offAnUpdateThread = new AtomicBoolean();
        final AtomicInteger nestedRuns = new AtomicInteger();
        final CountingContexts contexts = new CountingContexts();
        final Callbacks callbacks = new Callbacks();

        updateGraph.runWithinUnitTestCycle(() -> scheduler.iterateParallel(ExecutionContext.getContext(), null,
                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 1,
                (outerContext, outer, outerNec) -> scheduler.invokeParallel(ExecutionContext.getContext(), null,
                        contexts, 0, 2,
                        (context, idx, nec) -> {
                            if (!updateGraph.currentThreadProcessesUpdates()) {
                                offAnUpdateThread.set(true);
                            }
                            taskThreads.add(Thread.currentThread());
                            await(bothRunning);
                            scheduler.invokeParallel(ExecutionContext.getContext(), null,
                                    JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 5,
                                    (innerContext, inner, innerNec) -> nestedRuns.incrementAndGet());
                        }),
                callbacks.onComplete, callbacks.cleanup, callbacks.onError));

        assertThat(callbacks.error.get()).isNull();
        assertThat(callbacks.completeCalls.get()).isEqualTo(1);
        assertThat(taskThreads).hasSize(2);
        assertThat(offAnUpdateThread.get()).as("a task ran off the update threads").isFalse();
        assertThat(nestedRuns.get()).isEqualTo(10);
        assertThat(contexts.open.get()).isZero();
    }

    /** With a single update thread an invocation submits nothing, and that thread runs every task itself. */
    @Test
    public void testInvokeWithOneUpdateThreadRunsOnThatThread() {
        final ControlledUpdateGraph updateGraph = updateGraphWithUpdateThreads(1);
        final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
        final Set<Thread> taskThreads = ConcurrentHashMap.newKeySet();
        final AtomicReference<Thread> callerThread = new AtomicReference<>();
        final AtomicIntegerArray runs = new AtomicIntegerArray(8);
        final Callbacks callbacks = new Callbacks();

        updateGraph.runWithinUnitTestCycle(() -> scheduler.iterateParallel(ExecutionContext.getContext(), null,
                JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 1,
                (outerContext, outer, outerNec) -> {
                    callerThread.set(Thread.currentThread());
                    scheduler.invokeParallel(ExecutionContext.getContext(), null,
                            JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 8,
                            (context, idx, nec) -> {
                                taskThreads.add(Thread.currentThread());
                                runs.incrementAndGet(idx);
                            });
                },
                callbacks.onComplete, callbacks.cleanup, callbacks.onError));

        assertThat(callbacks.error.get()).isNull();
        assertThat(callbacks.completeCalls.get()).isEqualTo(1);
        assertThat(taskThreads).containsExactly(callerThread.get());
        assertEachRanOnce(runs);
    }

    /**
     * On the refresh thread, which alone dispatches notifications, an invocation's helpers wait undispatched, so the
     * caller runs every task itself. The helpers, flushed when the cycle completes, find their invokers taken.
     */
    @Test
    public void testInvokeOnTheRefreshThreadRunsEveryTaskItself() {
        final ControlledUpdateGraph updateGraph = updateGraphWithUpdateThreads(4);
        final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
        final AtomicReference<Thread> refreshThread = new AtomicReference<>();
        final AtomicBoolean callerIsAnUpdateThread = new AtomicBoolean();
        final Set<Thread> taskThreads = ConcurrentHashMap.newKeySet();
        final AtomicIntegerArray runs = new AtomicIntegerArray(8);
        final CountingContexts contexts = new CountingContexts();

        updateGraph.runWithinUnitTestCycle(() -> updateGraph.refreshUpdateSourceForUnitTests(() -> {
            refreshThread.set(Thread.currentThread());
            callerIsAnUpdateThread.set(updateGraph.currentThreadProcessesUpdates());
            scheduler.invokeParallel(ExecutionContext.getContext(), null, contexts, 0, 8,
                    (context, idx, nec) -> {
                        taskThreads.add(Thread.currentThread());
                        runs.incrementAndGet(idx);
                    });
        }));

        assertThat(callerIsAnUpdateThread.get()).isTrue();
        assertThat(taskThreads).containsExactly(refreshThread.get());
        assertEachRanOnce(runs);
        // the caller's own invoker and three helpers, every one closed by the caller
        assertThat(contexts.made.get()).isEqualTo(4);
        assertThat(contexts.open.get()).isZero();
    }

    /**
     * Outside an update cycle no job may be submitted, so with more than one update thread an invocation fails as it
     * starts, without running a task or leaving a context open. With a single update thread it submits nothing and runs
     * every task on the calling thread.
     */
    @Test
    public void testInvokeOutsideACycle() {
        final CountingContexts contexts = new CountingContexts();
        final AtomicInteger runs = new AtomicInteger();
        final JobScheduler pooled = new UpdateGraphJobScheduler(updateGraphWithUpdateThreads(4));
        assertThatThrownBy(() -> pooled.invokeParallel(ExecutionContext.getContext(), null, contexts, 0, 8,
                (context, idx, nec) -> runs.incrementAndGet()))
                .isInstanceOf(AssertionFailure.class);
        assertThat(runs.get()).isZero();
        assertThat(contexts.made.get()).isPositive();
        assertThat(contexts.open.get()).isZero();

        final JobScheduler single = new UpdateGraphJobScheduler(updateGraphWithUpdateThreads(1));
        final Set<Thread> taskThreads = ConcurrentHashMap.newKeySet();
        invoke(single, 8, (context, idx, nec) -> {
            taskThreads.add(Thread.currentThread());
            runs.incrementAndGet();
        });
        assertThat(runs.get()).isEqualTo(8);
        assertThat(taskThreads).containsExactly(Thread.currentThread());
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
