//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a thread waiting for an {@link OperationInitializerJobScheduler}'s jobs runs the ones no pool thread has
 * started, so that it finishes even while every pool thread is blocked.
 */
public class TestOperationInitializerJobScheduler {

    private static final int POOL_THREADS = 2;
    private static final long TIMEOUT_SECONDS = 30;

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    private OperationInitializationThreadPool pool;
    private final CountDownLatch releasePool = new CountDownLatch(1);

    @Before
    public void setUp() {
        pool = new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, POOL_THREADS);
    }

    @After
    public void tearDown() {
        releasePool.countDown();
        pool.shutdown();
    }

    /** Occupy every pool thread until the test ends, so that jobs submitted to the pool stay queued. */
    private void blockPool() throws InterruptedException {
        final CountDownLatch blocked = new CountDownLatch(POOL_THREADS);
        for (int ii = 0; ii < POOL_THREADS; ++ii) {
            pool.submit(() -> {
                blocked.countDown();
                try {
                    releasePool.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        assertThat(blocked.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    /** Run {@code test} on another thread under this thread's execution context, failing if it does not return. */
    private static void withTimeout(final ThrowingRunnable test) throws InterruptedException {
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                test.run();
            } catch (final Throwable t) {
                thrown.set(t);
            }
        }, "operation-initializer-job-scheduler-test");
        thread.setDaemon(true);
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        assertThat(thread.isAlive()).as("the waiting thread finished").isFalse();
        if (thrown.get() != null) {
            throw new AssertionError(thrown.get());
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * With every pool thread blocked, a callback-form iteration and the jobs it submits from its callbacks run on the
     * thread waiting for its completion, and reading the performance afterwards has nothing left to wait for.
     */
    @Test
    public void testAwaitCompletionRunsQueuedJobsWhilePoolIsBlocked() throws InterruptedException {
        blockPool();
        withTimeout(() -> {
            final OperationInitializerJobScheduler scheduler = new OperationInitializerJobScheduler(pool);
            final Set<Thread> taskThreads = ConcurrentHashMap.newKeySet();
            final AtomicInteger followUps = new AtomicInteger();
            final CompletableFuture<Void> done = new CompletableFuture<>();
            scheduler.iterateParallel(ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0, 8,
                    (context, idx, nec) -> taskThreads.add(Thread.currentThread()),
                    // a job submitted from a callback, after the waiting thread has begun to wait
                    () -> scheduler.submit(ExecutionContext.getContext(), () -> {
                        followUps.incrementAndGet();
                        done.complete(null);
                    }, null, done::completeExceptionally),
                    () -> {
                    },
                    done::completeExceptionally);
            scheduler.awaitCompletion(done);
            assertThat(scheduler.getAccumulatedPerformance()).isNotNull();
            assertThat(taskThreads).containsExactly(Thread.currentThread());
            assertThat(followUps.get()).isEqualTo(1);
        });
    }

    /**
     * After an invocation whose caller ran every task, the jobs of the invokers the caller took over are withdrawn, so
     * reading the performance does not wait for the pool to dequeue them.
     */
    @Test
    public void testPerformanceAfterInvokeParallelDoesNotWaitForQueuedJobs() throws InterruptedException {
        blockPool();
        withTimeout(() -> {
            final OperationInitializerJobScheduler scheduler = new OperationInitializerJobScheduler(pool);
            final AtomicInteger runs = new AtomicInteger();
            scheduler.invokeParallel(ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 8,
                    (context, idx, nec) -> runs.incrementAndGet());
            assertThat(runs.get()).isEqualTo(8);
            assertThat(scheduler.getAccumulatedPerformance()).isNotNull();
        });
    }

    /**
     * A job the waiting thread runs fails the future, which the wait reports as {@link java.util.concurrent.Future}.
     */
    @Test
    public void testAwaitCompletionReportsAFailedJob() throws InterruptedException {
        blockPool();
        withTimeout(() -> {
            final OperationInitializerJobScheduler scheduler = new OperationInitializerJobScheduler(pool);
            final IllegalStateException failure = new IllegalStateException("job failed");
            final CompletableFuture<Void> done = new CompletableFuture<>();
            scheduler.submit(ExecutionContext.getContext(), () -> {
                throw failure;
            }, null, done::completeExceptionally);
            assertThatThrownBy(() -> scheduler.awaitCompletion(done))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseReference(failure);
            assertThat(scheduler.getAccumulatedPerformance()).isNotNull();
        });
    }

    /**
     * A job already running on a pool thread is not run again: the waiting thread waits for it to complete the future.
     */
    @Test
    public void testAwaitCompletionWaitsForAJobRunningOnThePool() throws InterruptedException {
        withTimeout(() -> {
            final OperationInitializerJobScheduler scheduler = new OperationInitializerJobScheduler(pool);
            final CountDownLatch running = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final AtomicInteger runs = new AtomicInteger();
            final CompletableFuture<Thread> done = new CompletableFuture<>();
            scheduler.submit(ExecutionContext.getContext(), () -> {
                runs.incrementAndGet();
                running.countDown();
                try {
                    release.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                done.complete(Thread.currentThread());
            }, null, done::completeExceptionally);
            assertThat(running.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            final Thread releaser = new Thread(() -> {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                release.countDown();
            });
            releaser.start();
            assertThat(scheduler.awaitCompletion(done)).isNotSameAs(Thread.currentThread());
            assertThat(scheduler.getAccumulatedPerformance()).isNotNull();
            assertThat(runs.get()).isEqualTo(1);
        });
    }

    /**
     * An interrupt cancels the future, so that a job checking it does no work, but the wait still runs every queued job
     * before throwing, leaving the performance read nothing to wait for.
     */
    @Test
    public void testInterruptCancelsTheFutureAndFinishesTheJobs() throws InterruptedException {
        blockPool();
        withTimeout(() -> {
            final OperationInitializerJobScheduler scheduler = new OperationInitializerJobScheduler(pool);
            final CompletableFuture<Void> never = new CompletableFuture<>();
            final AtomicInteger runs = new AtomicInteger();
            final AtomicInteger worked = new AtomicInteger();
            scheduler.submit(ExecutionContext.getContext(), () -> {
                runs.incrementAndGet();
                if (!never.isDone()) {
                    worked.incrementAndGet();
                }
            }, null, never::completeExceptionally);
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> scheduler.awaitCompletion(never)).isInstanceOf(InterruptedException.class);
            assertThat(never.isCancelled()).isTrue();
            assertThat(runs.get()).isEqualTo(1);
            assertThat(worked.get()).isZero();
            assertThat(scheduler.getAccumulatedPerformance()).isNotNull();
        });
    }

    /**
     * A job that completes the future and goes on running keeps the wait going until it finishes, so the performance
     * read that follows has nothing to wait for.
     */
    @Test
    public void testAwaitCompletionWaitsForJobsStillRunningAfterTheFuture() throws InterruptedException {
        withTimeout(() -> {
            final OperationInitializerJobScheduler scheduler = new OperationInitializerJobScheduler(pool);
            final CompletableFuture<Void> done = new CompletableFuture<>();
            final CountDownLatch release = new CountDownLatch(1);
            final AtomicInteger finished = new AtomicInteger();
            scheduler.submit(ExecutionContext.getContext(), () -> {
                done.complete(null);
                try {
                    release.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                finished.incrementAndGet();
            }, null, done::completeExceptionally);
            assertThat(done.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isNull();
            final Thread releaser = new Thread(() -> {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                release.countDown();
            });
            releaser.start();
            scheduler.awaitCompletion(done);
            assertThat(finished.get()).isEqualTo(1);
        });
    }

    /** An inline initializer runs each job inside submit, so there is nothing left for the waiting thread to run. */
    @Test
    public void testInlineInitializerLeavesNothingToRun() throws InterruptedException, ExecutionException {
        final OperationInitializerJobScheduler scheduler =
                new OperationInitializerJobScheduler(OperationInitializer.NON_PARALLELIZABLE);
        final CompletableFuture<Integer> done = new CompletableFuture<>();
        scheduler.submit(null, () -> done.complete(42), null, done::completeExceptionally);
        assertThat(done.isDone()).isTrue();
        assertThat(scheduler.awaitCompletion(done)).isEqualTo(42);
        assertThat(scheduler.getAccumulatedPerformance()).isNotNull();
    }
}
