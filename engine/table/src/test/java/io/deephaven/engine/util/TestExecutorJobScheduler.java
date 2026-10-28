//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.util.ExecutorJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of {@link ExecutorJobScheduler#submit} and {@link ExecutorJobScheduler#newHelperPool}. The iteration behavior
 * that the scheduler exists for is covered by {@link TestInvokeParallel}.
 */
public class TestExecutorJobScheduler {

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

    private ThreadPoolExecutor newPool(final int maxThreads) {
        final ThreadPoolExecutor pool = ExecutorJobScheduler.newHelperPool(maxThreads, runnable -> {
            final Thread thread = new Thread(runnable, "executor-job-scheduler-test");
            thread.setDaemon(true);
            return thread;
        });
        pools.add(pool);
        return pool;
    }

    private static void await(final CountDownLatch latch) throws InterruptedException {
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    /** The scheduler's outstanding job count, which nothing else exposes. */
    private static int outstandingJobs(final ExecutorJobScheduler scheduler) {
        try {
            final java.lang.reflect.Field field = ExecutorJobScheduler.class.getDeclaredField("outstandingJobs");
            field.setAccessible(true);
            return ((AtomicInteger) field.get(scheduler)).get();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testHelperPoolShape() {
        final ThreadPoolExecutor pool = newPool(3);
        assertThat(pool.getCorePoolSize()).isZero();
        assertThat(pool.getMaximumPoolSize()).isEqualTo(3);
        assertThat(pool.getQueue()).isInstanceOf(SynchronousQueue.class);
        assertThat(pool.getKeepAliveTime(TimeUnit.SECONDS)).isEqualTo(60);

        assertThatThrownBy(() -> ExecutorJobScheduler.newHelperPool(0, Thread::new))
                .isInstanceOf(RequirementFailure.class);
        assertThatThrownBy(() -> new ExecutorJobScheduler(pool, 0))
                .isInstanceOf(RequirementFailure.class);
    }

    @Test
    public void testThreadCountIsWhatWasGiven() {
        assertThat(new ExecutorJobScheduler(newPool(2), 7).threadCount()).isEqualTo(7);
        assertThat(new ExecutorJobScheduler(newPool(2), 1).threadCount()).isEqualTo(1);
    }

    @Test
    public void testAcceptedJobRunsOnAPoolThreadAndIsAccountedFor() throws InterruptedException {
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(newPool(1), 2);
        final CountDownLatch ran = new CountDownLatch(1);
        final AtomicReference<Thread> runningThread = new AtomicReference<>();
        final AtomicReference<ExecutionContext> runningContext = new AtomicReference<>();
        final ExecutionContext callerContext = ExecutionContext.getContext();

        scheduler.submit(callerContext, () -> {
            runningThread.set(Thread.currentThread());
            runningContext.set(ExecutionContext.getContext());
            ran.countDown();
        }, null, e -> {
            throw new AssertionError("unexpected failure", e);
        });
        await(ran);

        assertThat(runningThread.get()).isNotSameAs(Thread.currentThread());
        assertThat(runningContext.get()).isSameAs(callerContext);
        assertThat(scheduler.getAccumulatedPerformance()).isNotNull();
    }

    @Test
    public void testRefusedJobRunsOnTheSubmittingThread() {
        final Executor refusing = command -> {
            throw new RejectedExecutionException("no threads to spare");
        };
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(refusing, 4);
        final AtomicReference<Thread> runningThread = new AtomicReference<>();

        scheduler.submit(null, () -> runningThread.set(Thread.currentThread()), null, e -> {
            throw new AssertionError("unexpected failure", e);
        });

        assertThat(runningThread.get()).isSameAs(Thread.currentThread());
        // the refused job released its count, and its run here is this thread's own work
        assertThat(outstandingJobs(scheduler)).isZero();
        assertThat(scheduler.getAccumulatedPerformance().getUsageNanos()).isZero();
    }

    /** A submission the executor fails outright releases its count, so that reading the performance does not hang. */
    @Test
    public void testFailedSubmissionReleasesItsCount() {
        final IllegalStateException broken = new IllegalStateException("cannot start a thread");
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(command -> {
            throw broken;
        }, 4);

        assertThatThrownBy(() -> scheduler.submit(null, () -> {
        }, null, e -> {
        })).isSameAs(broken);

        assertThat(outstandingJobs(scheduler)).isZero();
    }

    @Test
    public void testBusyPoolRefusesAndTheSubmitterRunsTheJob() throws InterruptedException {
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(newPool(1), 2);
        final CountDownLatch holdTheThread = new CountDownLatch(1);
        final CountDownLatch holding = new CountDownLatch(1);
        scheduler.submit(null, () -> {
            holding.countDown();
            try {
                holdTheThread.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, null, e -> {
        });
        await(holding);

        // The pool's only thread is busy, so this job is refused and runs here
        final AtomicReference<Thread> runningThread = new AtomicReference<>();
        scheduler.submit(null, () -> runningThread.set(Thread.currentThread()), null, e -> {
        });
        assertThat(runningThread.get()).isSameAs(Thread.currentThread());
        holdTheThread.countDown();
    }

    /**
     * A synchronous executor that runs the job, which then throws a RejectedExecutionException itself, has not refused
     * it: the job must not run a second time on the submitting thread.
     */
    @Test
    public void testRejectionThrownByARunJobDoesNotRunItAgain() {
        final Executor direct = Runnable::run;
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(direct, 2);
        final AtomicInteger runs = new AtomicInteger();
        final RejectedExecutionException fromTheJob = new RejectedExecutionException("thrown by the job's handler");

        assertThatThrownBy(() -> scheduler.submit(null, () -> {
            runs.incrementAndGet();
            throw new IllegalStateException("job failed");
        }, null, e -> {
            throw fromTheJob;
        })).isSameAs(fromTheJob);

        assertThat(runs.get()).isEqualTo(1);
        // released once, by the job, and not again for the rejection
        assertThat(outstandingJobs(scheduler)).isZero();
    }

    /** Waiting for outstanding jobs keeps going through an interrupt, and restores it for the caller. */
    @Test
    public void testPerformanceWaitRestoresTheInterrupt() throws InterruptedException {
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch running = new CountDownLatch(1);
        final OperationInitializerJobScheduler scheduler =
                new OperationInitializerJobScheduler(ExecutionContext.getContext().getOperationInitializer());
        scheduler.submit(null, () -> {
            running.countDown();
            try {
                release.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, null, e -> {
        });
        await(running);

        final AtomicReference<Boolean> interruptedAfter = new AtomicReference<>();
        final Thread waiter = new Thread(() -> {
            scheduler.getAccumulatedPerformance();
            interruptedAfter.set(Thread.currentThread().isInterrupted());
        }, "performance-waiter");
        waiter.setDaemon(true);
        waiter.start();
        // let the waiter block, interrupt it, then let the job finish
        Thread.sleep(100);
        waiter.interrupt();
        Thread.sleep(50);
        release.countDown();
        waiter.join(10_000);

        assertThat(waiter.isAlive()).isFalse();
        assertThat(interruptedAfter.get()).isTrue();
    }

    /** Reading the performance waits for a job running on a pool thread to finish and add its own. */
    @Test
    public void testPerformanceWaitsForStartedJobs() throws InterruptedException {
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(newPool(1), 2);
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        scheduler.submit(null, () -> {
            running.countDown();
            try {
                release.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, null, e -> {
        });
        await(running);

        final AtomicReference<BasePerformanceEntry> read = new AtomicReference<>();
        final Thread reader = new Thread(() -> read.set(scheduler.getAccumulatedPerformance()), "performance-reader");
        reader.setDaemon(true);
        reader.start();
        reader.join(200);
        assertThat(reader.isAlive()).as("still waiting for the running job").isTrue();
        release.countDown();
        reader.join(10_000);

        assertThat(reader.isAlive()).isFalse();
        // the job ran for at least the 200 ms the reader was kept waiting
        assertThat(read.get().getUsageNanos()).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(200));
    }

    /**
     * The wait covers every job submitted, as {@link OperationInitializerJobScheduler}'s does, including one the
     * executor has yet to run: after an invocation over a queueing executor, whose caller ran every task itself,
     * reading the performance waits until the queued jobs, which then do nothing, have run.
     */
    @Test
    public void testPerformanceWaitsForJobsNotYetRun() throws InterruptedException {
        final List<Runnable> queued = Collections.synchronizedList(new ArrayList<>());
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(queued::add, 4);
        final AtomicInteger runs = new AtomicInteger();
        scheduler.invokeParallel(ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 8,
                (context, idx, nec) -> runs.incrementAndGet());
        assertThat(runs.get()).isEqualTo(8);
        assertThat(queued).hasSize(3);

        final Thread reader = new Thread(scheduler::getAccumulatedPerformance, "performance-reader");
        reader.setDaemon(true);
        reader.start();
        reader.join(200);
        assertThat(reader.isAlive()).as("still waiting for the queued jobs").isTrue();
        // the late jobs find their invokers taken, and do nothing
        queued.forEach(Runnable::run);
        reader.join(10_000);

        assertThat(reader.isAlive()).isFalse();
        assertThat(runs.get()).isEqualTo(8);
    }

    /**
     * A job a synchronous executor runs on the submitting thread is that thread's own work, as a refused job is, and is
     * not accounted again by the scheduler.
     */
    @Test
    public void testJobRunOnTheSubmittingThreadIsNotAccounted() {
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(Runnable::run, 2);
        final AtomicReference<Thread> runningThread = new AtomicReference<>();

        scheduler.submit(null, () -> {
            runningThread.set(Thread.currentThread());
            try {
                Thread.sleep(20);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, null, e -> {
        });

        assertThat(runningThread.get()).isSameAs(Thread.currentThread());
        assertThat(outstandingJobs(scheduler)).isZero();
        assertThat(scheduler.getAccumulatedPerformance().getUsageNanos()).isZero();
    }

    @Test
    public void testExceptionIsDeliveredToOnError() throws InterruptedException {
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(newPool(1), 2);
        final IllegalStateException failure = new IllegalStateException("job failed");
        final CountDownLatch delivered = new CountDownLatch(1);
        final AtomicReference<Exception> deliveredException = new AtomicReference<>();

        scheduler.submit(null, () -> {
            throw failure;
        }, null, e -> {
            deliveredException.set(e);
            delivered.countDown();
        });
        await(delivered);

        assertThat(deliveredException.get()).isSameAs(failure);
    }

    @Test
    public void testErrorIsDeliveredWrappedBeforeItPropagates() throws InterruptedException {
        // A thread of its own, so that the fatal report the pool thread makes is observable through its uncaught
        // exception handler rather than lost
        final AtomicReference<Throwable> escaped = new AtomicReference<>();
        final CountDownLatch threadDone = new CountDownLatch(1);
        final ThreadPoolExecutor pool = ExecutorJobScheduler.newHelperPool(1, runnable -> {
            final Thread thread = new Thread(() -> {
                try {
                    runnable.run();
                } catch (final Throwable t) {
                    escaped.set(t);
                } finally {
                    threadDone.countDown();
                }
            }, "executor-job-scheduler-error-test");
            thread.setDaemon(true);
            return thread;
        });
        pools.add(pool);
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(pool, 2);
        final AssertionError error = new AssertionError("job error");
        final CountDownLatch delivered = new CountDownLatch(1);
        final AtomicReference<Exception> deliveredException = new AtomicReference<>();

        scheduler.submit(null, () -> {
            throw error;
        }, logOutput -> logOutput.append("error test"), e -> {
            deliveredException.set(e);
            delivered.countDown();
        });
        await(delivered);
        await(threadDone);

        assertThat(deliveredException.get()).isInstanceOf(UncheckedDeephavenException.class);
        assertThat(deliveredException.get().getCause()).isSameAs(error);
        // the unit test fatal error reporter throws, and that is what escaped the job; in production the report is
        // made and the Error itself propagates to the pool
        assertThat(escaped.get()).isNotNull();
        assertThat(JobScheduler.asDeliverableException(error).getCause()).isSameAs(error);
    }
}
