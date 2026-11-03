//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.testutil.testcase.FakeProcessEnvironment;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.util.ExecutorJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraphWaitGuard;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of {@link ExecutorJobScheduler#submit}, {@link ExecutorJobScheduler#awaitCompletion} and
 * {@link ExecutorJobScheduler#newHelperPool}, and of {@link OperationInitializerJobScheduler}, which is an
 * {@link ExecutorJobScheduler} over its initializer. The iteration behavior that the schedulers exist for is covered by
 * {@link TestInvokeParallel}.
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
        assertThat(pool.getCorePoolSize()).isEqualTo(3);
        assertThat(pool.getMaximumPoolSize()).isEqualTo(3);
        assertThat(pool.getQueue()).isInstanceOf(LinkedBlockingQueue.class);
        assertThat(pool.getKeepAliveTime(TimeUnit.SECONDS)).isEqualTo(60);
        assertThat(pool.allowsCoreThreadTimeOut()).isTrue();

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

    /** A job the executor refuses is withdrawn, never runs, and the refusal reaches the submitter. */
    @Test
    public void testRefusedJobIsWithdrawnAndTheRefusalRethrown() {
        final RejectedExecutionException refusal = new RejectedExecutionException("shut down");
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(command -> {
            throw refusal;
        }, 4);
        final AtomicInteger runs = new AtomicInteger();

        assertThatThrownBy(() -> scheduler.submit(null, runs::incrementAndGet, null, e -> {
            throw new AssertionError("unexpected failure", e);
        })).isSameAs(refusal);

        assertThat(runs.get()).isZero();
        assertThat(outstandingJobs(scheduler)).isZero();
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

    /** A job submitted while every thread of the pool is busy waits for one to finish, and then runs on it. */
    @Test
    public void testBusyPoolQueuesTheJobForTheFirstThreadToFinish() throws InterruptedException {
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(newPool(1), 2);
        final CountDownLatch holdTheThread = new CountDownLatch(1);
        final CountDownLatch holding = new CountDownLatch(1);
        final AtomicReference<Thread> poolThread = new AtomicReference<>();
        scheduler.submit(null, () -> {
            poolThread.set(Thread.currentThread());
            holding.countDown();
            try {
                holdTheThread.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, null, e -> {
        });
        await(holding);

        final AtomicReference<Thread> runningThread = new AtomicReference<>();
        final CountDownLatch ran = new CountDownLatch(1);
        scheduler.submit(null, () -> {
            runningThread.set(Thread.currentThread());
            ran.countDown();
        }, null, e -> {
        });
        assertThat(runningThread.get()).as("queued, not run by the submitter").isNull();
        holdTheThread.countDown();
        await(ran);
        assertThat(runningThread.get()).isSameAs(poolThread.get());
    }

    /**
     * An invocation whose helpers are all queued behind other work while it starts is helped by each helper thread as
     * it frees up, rather than left to the calling thread.
     */
    @Test
    public void testInvocationIsHelpedByThreadsThatFreeUpWhileItRuns() throws InterruptedException {
        final int helpers = 2;
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(newPool(helpers), helpers + 1);
        final CountDownLatch holdTheThreads = new CountDownLatch(1);
        final CountDownLatch holding = new CountDownLatch(helpers);
        final ExecutorJobScheduler otherCaller = new ExecutorJobScheduler(pools.get(0), helpers + 1);
        for (int ii = 0; ii < helpers; ++ii) {
            otherCaller.submit(null, () -> {
                holding.countDown();
                try {
                    holdTheThreads.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, null, e -> {
            });
        }
        await(holding);

        final Thread caller = Thread.currentThread();
        final CountDownLatch helped = new CountDownLatch(1);
        final AtomicReference<Boolean> helpedWhileTheCallerWaited = new AtomicReference<>();
        scheduler.invokeParallel(ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 16,
                (context, idx, nec) -> {
                    if (Thread.currentThread() != caller) {
                        helped.countDown();
                    } else if (helpedWhileTheCallerWaited.get() == null) {
                        // the caller's first task frees the pool, then waits for a freed thread to take a task
                        holdTheThreads.countDown();
                        try {
                            helpedWhileTheCallerWaited.set(helped.await(10, TimeUnit.SECONDS));
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
        assertThat(helpedWhileTheCallerWaited.get()).isTrue();
    }

    /**
     * A callback-form iteration with more tasks than threads returns once its jobs are submitted, with none of its
     * tasks run on the calling thread, even while every thread of the pool is busy.
     */
    @Test
    public void testCallbackIterationDoesNotRunTasksOnTheCallingThread() throws InterruptedException {
        final int helpers = 2;
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(newPool(helpers), helpers + 1);
        final Thread caller = Thread.currentThread();
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger onCaller = new AtomicInteger();
        final AtomicInteger run = new AtomicInteger();
        final CountDownLatch completed = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        scheduler.iterateParallel(ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 16,
                (context, idx, nec) -> {
                    if (Thread.currentThread() == caller) {
                        onCaller.incrementAndGet();
                    }
                    try {
                        // held until the iteration has returned, which it does only if no task runs on the caller
                        assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    run.incrementAndGet();
                },
                completed::countDown,
                () -> {
                },
                e -> {
                    failure.set(e);
                    completed.countDown();
                });
        release.countDown();
        await(completed);
        assertThat(failure.get()).isNull();
        assertThat(onCaller.get()).isZero();
        assertThat(run.get()).isEqualTo(16);
    }

    /**
     * A synchronous executor that runs the job, whose error handler then throws a RejectedExecutionException, has not
     * refused it: the handler's failure is reported as fatal, and the job, which has run and released its count, is not
     * withdrawn as well.
     */
    @Test
    public void testRejectionThrownByARunJobDoesNotRunItAgain() {
        final Executor direct = Runnable::run;
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(direct, 2);
        final AtomicInteger runs = new AtomicInteger();
        final IllegalStateException jobFailure = new IllegalStateException("job failed");
        final RejectedExecutionException fromTheJob = new RejectedExecutionException("thrown by the job's handler");

        assertThatThrownBy(() -> scheduler.submit(null, () -> {
            runs.incrementAndGet();
            throw jobFailure;
        }, null, e -> {
            throw fromTheJob;
        })).isInstanceOf(FakeProcessEnvironment.FakeFatalException.class).hasCause(fromTheJob);

        // the job's failure has no other way out, so the handler's failure carries it
        assertThat(fromTheJob.getSuppressed()).containsExactly(jobFailure);
        assertThat(runs.get()).isEqualTo(1);
        // released once, by the job, and not again for the rejection
        assertThat(outstandingJobs(scheduler)).isZero();
    }

    /** Waiting for outstanding jobs keeps going through an interrupt, and restores it for the caller. */
    @Test
    public void testPerformanceWaitRestoresTheInterrupt() throws InterruptedException {
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch running = new CountDownLatch(1);
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(newPool(1), 2);
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
     * After an invocation over a queueing executor, whose caller ran every task itself, the jobs still queued have been
     * withdrawn: reading the performance does not wait for them, and they do nothing once they run.
     */
    @Test
    public void testInvocationWithdrawsJobsNotYetRun() throws InterruptedException {
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
        reader.join(10_000);
        assertThat(reader.isAlive()).isFalse();

        queued.forEach(Runnable::run);
        assertThat(runs.get()).isEqualTo(8);
    }

    /**
     * A job a synchronous executor runs on the submitting thread is that thread's own work, and is not accounted again
     * by the scheduler; neither is a job submitted from inside one, which runs inline too.
     */
    @Test
    public void testJobRunOnTheSubmittingThreadIsNotAccounted() {
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(Runnable::run, 2);
        final Set<Thread> runningThreads = ConcurrentHashMap.newKeySet();
        final AtomicInteger runs = new AtomicInteger();
        final Runnable sleep = () -> {
            runningThreads.add(Thread.currentThread());
            runs.incrementAndGet();
            try {
                Thread.sleep(20);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        scheduler.submit(null, () -> {
            sleep.run();
            scheduler.submit(null, sleep, null, e -> {
            });
        }, null, e -> {
        });

        assertThat(runs.get()).isEqualTo(2);
        assertThat(runningThreads).containsExactly(Thread.currentThread());
        assertThat(outstandingJobs(scheduler)).isZero();
        assertThat(scheduler.getAccumulatedPerformance().getUsageNanos()).isZero();
    }

    /**
     * A job queued by the executor and run later on the thread that submitted it, as a pool thread may run the job it
     * submitted, is not that thread's own work, and the scheduler accounts it.
     */
    @Test
    public void testQueuedJobRunLaterOnTheSubmittingThreadIsAccounted() {
        final List<Runnable> queued = new ArrayList<>();
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(queued::add, 2);
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
        assertThat(queued).hasSize(1);
        queued.forEach(Runnable::run);

        assertThat(runningThread.get()).isSameAs(Thread.currentThread());
        assertThat(outstandingJobs(scheduler)).isZero();
        assertThat(scheduler.getAccumulatedPerformance().getUsageNanos())
                .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(20));
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

    /**
     * A thread waiting for a callback-form iteration runs the jobs the executor has not started, those submitted from
     * the iteration's callbacks included, as its own work; the executor's entries for them then do nothing.
     */
    @Test
    public void testAwaitCompletionRunsJobsTheExecutorHasNotStarted() throws InterruptedException, ExecutionException {
        final List<Runnable> queued = new ArrayList<>();
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(queued::add, 4);
        final Set<Thread> taskThreads = ConcurrentHashMap.newKeySet();
        final AtomicInteger followUps = new AtomicInteger();
        final CompletableFuture<Void> done = new CompletableFuture<>();
        scheduler.iterateParallel(ExecutionContext.getContext(), null, JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 8,
                (context, idx, nec) -> taskThreads.add(Thread.currentThread()),
                // a job submitted from a callback, after the waiting thread has begun to wait
                () -> scheduler.submit(ExecutionContext.getContext(), () -> {
                    followUps.incrementAndGet();
                    done.complete(null);
                }, null, done::completeExceptionally),
                () -> {
                },
                done::completeExceptionally);
        assertThat(queued).isNotEmpty();

        assertThat(scheduler.awaitCompletion(done)).isNull();
        assertThat(taskThreads).containsExactly(Thread.currentThread());
        assertThat(followUps.get()).isEqualTo(1);
        assertThat(outstandingJobs(scheduler)).isZero();
        assertThat(scheduler.getAccumulatedPerformance().getUsageNanos()).isZero();

        final int runnersBefore = taskThreads.size();
        new ArrayList<>(queued).forEach(Runnable::run);
        assertThat(taskThreads).hasSize(runnersBefore);
        assertThat(followUps.get()).isEqualTo(1);
    }

    /** The wait goes on until a job a pool thread started has finished, even once the future is done. */
    @Test
    public void testAwaitCompletionWaitsForAJobStillRunningAfterTheFuture() throws Exception {
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(newPool(1), 2);
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
        assertThat(done.get(10, TimeUnit.SECONDS)).isNull();
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
        releaser.join();
    }

    /**
     * An interrupt cancels the future, so that a job that checks it stops early, but the waiting thread still runs the
     * job before it throws.
     */
    @Test
    public void testInterruptCancelsTheFutureAndFinishesTheJobs() {
        final List<Runnable> queued = new ArrayList<>();
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(queued::add, 2);
        final CompletableFuture<Void> done = new CompletableFuture<>();
        final AtomicReference<Boolean> sawCancelled = new AtomicReference<>();
        scheduler.submit(null, () -> sawCancelled.set(done.isCancelled()), null, done::completeExceptionally);

        Thread.currentThread().interrupt();
        assertThatThrownBy(() -> scheduler.awaitCompletion(done)).isInstanceOf(InterruptedException.class);
        assertThat(sawCancelled.get()).isTrue();
        assertThat(outstandingJobs(scheduler)).isZero();
    }

    /**
     * A job that waits for its own scheduler would wait for itself, so the wait fails the job instead, whether a
     * synchronous executor, the pool or the waiting thread runs it.
     */
    @Test
    public void testJobWaitingForItsOwnSchedulerFails() throws InterruptedException {
        final List<Runnable> queued = new ArrayList<>();
        final List<Executor> executors = List.of(Runnable::run, newPool(2), queued::add);
        for (final Executor executor : executors) {
            final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(executor, 2);
            final CompletableFuture<Void> done = new CompletableFuture<>();
            scheduler.submit(ExecutionContext.getContext(), () -> {
                try {
                    scheduler.awaitCompletion(new CompletableFuture<Void>());
                    done.complete(null);
                } catch (final InterruptedException | ExecutionException e) {
                    done.completeExceptionally(e);
                }
            }, null, done::completeExceptionally);
            assertThatThrownBy(() -> scheduler.awaitCompletion(done))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(AssertionFailure.class);
            assertThat(outstandingJobs(scheduler)).isZero();
        }
    }

    /**
     * A job nested inside another of the scheduler's jobs on the same thread, as a fork/join pool's thread runs one
     * when it helps the pool quiesce, is part of the enclosing job, whose performance already holds its time.
     */
    @Test
    public void testJobNestedInAnotherOnTheSameThreadIsNotAccountedAgain() {
        final List<Runnable> queued = new ArrayList<>();
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(queued::add, 2);
        final long nestedSleepMillis = 200;
        final AtomicInteger nestedRuns = new AtomicInteger();

        scheduler.submit(null, () -> {
            scheduler.submit(null, () -> {
                nestedRuns.incrementAndGet();
                try {
                    Thread.sleep(nestedSleepMillis);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, null, e -> {
            });
            // run the nested job's queued entry here, inside the enclosing job
            queued.remove(queued.size() - 1).run();
        }, null, e -> {
        });
        assertThat(queued).hasSize(1);
        final long startNanos = System.nanoTime();
        queued.remove(0).run();
        final long elapsedNanos = System.nanoTime() - startNanos;

        assertThat(nestedRuns.get()).isEqualTo(1);
        assertThat(outstandingJobs(scheduler)).isZero();
        // counted once, by the enclosing job, and not twice: no more than the time the enclosing job took
        assertThat(scheduler.getAccumulatedPerformance().getUsageNanos())
                .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(nestedSleepMillis))
                .isLessThanOrEqualTo(elapsedNanos);
    }

    /** A job may not wait for other threads when the thread that submitted it may not. */
    @Test
    public void testJobInheritsTheSubmittersWaitGuard() throws InterruptedException, ExecutionException {
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(newPool(1), 2);
        for (final boolean forbidden : new boolean[] {false, true}) {
            final CompletableFuture<Boolean> sawForbidden = new CompletableFuture<>();
            try (final SafeCloseable ignored = forbidden ? UpdateGraphWaitGuard.forbidWaits() : null) {
                scheduler.submit(null, () -> sawForbidden.complete(UpdateGraphWaitGuard.waitsForbidden()), null,
                        sawForbidden::completeExceptionally);
            }
            assertThat(scheduler.awaitCompletion(sawForbidden)).isEqualTo(forbidden);
        }
    }

    /** A job the waiting thread runs fails the future, which the wait reports as an {@link ExecutionException}. */
    @Test
    public void testAwaitCompletionReportsAFailedJob() {
        final List<Runnable> queued = new ArrayList<>();
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(queued::add, 2);
        final IllegalStateException failure = new IllegalStateException("job failed");
        final CompletableFuture<Void> done = new CompletableFuture<>();
        scheduler.submit(ExecutionContext.getContext(), () -> {
            throw failure;
        }, null, done::completeExceptionally);
        assertThat(queued).hasSize(1);

        assertThatThrownBy(() -> scheduler.awaitCompletion(done))
                .isInstanceOf(ExecutionException.class)
                .hasCauseReference(failure);
        assertThat(outstandingJobs(scheduler)).isZero();
    }

    /**
     * A job already running on a pool thread is not run again: the waiting thread waits for it to complete the future.
     */
    @Test
    public void testAwaitCompletionWaitsForAJobRunningOnThePool() throws Exception {
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(newPool(1), 2);
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
        await(running);
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
        assertThat(runs.get()).isEqualTo(1);
        assertThat(outstandingJobs(scheduler)).isZero();
        releaser.join();
    }

    /** A synchronous executor runs each job inside submit, so there is nothing left for the waiting thread to run. */
    @Test
    public void testSynchronousExecutorLeavesTheWaitNothingToRun() throws InterruptedException, ExecutionException {
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(Runnable::run, 2);
        final CompletableFuture<Integer> done = new CompletableFuture<>();
        scheduler.submit(null, () -> done.complete(42), null, done::completeExceptionally);
        assertThat(done.isDone()).isTrue();
        assertThat(scheduler.awaitCompletion(done)).isEqualTo(42);
        assertThat(outstandingJobs(scheduler)).isZero();
    }

    /**
     * An {@link OperationInitializerJobScheduler}'s thread count is its initializer's parallelism, read each time it is
     * asked for, so that constructing one over an initializer that cannot answer yet does not fail.
     */
    @Test
    public void testOperationInitializerThreadCountIsReadWhenAskedFor() {
        final AtomicInteger parallelism = new AtomicInteger(-1);
        final OperationInitializer initializer = new OperationInitializer() {
            @Override
            public boolean canParallelize() {
                return true;
            }

            @Override
            public Future<?> submit(final Runnable runnable) {
                runnable.run();
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public int parallelismFactor() {
                final int value = parallelism.get();
                if (value < 0) {
                    throw new IllegalStateException("no parallelism yet");
                }
                return value;
            }
        };
        final OperationInitializerJobScheduler scheduler = new OperationInitializerJobScheduler(initializer);
        assertThatThrownBy(scheduler::threadCount).isInstanceOf(IllegalStateException.class);
        parallelism.set(3);
        assertThat(scheduler.threadCount()).isEqualTo(3);
        parallelism.set(5);
        assertThat(scheduler.threadCount()).isEqualTo(5);
    }

    /**
     * With every thread of an {@link OperationInitializationThreadPool} blocked, a callback-form iteration and the jobs
     * it submits from its callbacks run on the thread waiting for its completion, which therefore finishes.
     */
    @Test
    public void testOperationInitializerWaitFinishesWhilePoolIsBlocked() throws InterruptedException {
        final int poolThreads = 2;
        final OperationInitializationThreadPool pool =
                new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, poolThreads);
        final CountDownLatch releasePool = new CountDownLatch(1);
        try {
            final CountDownLatch blocked = new CountDownLatch(poolThreads);
            for (int ii = 0; ii < poolThreads; ++ii) {
                pool.submit(() -> {
                    blocked.countDown();
                    try {
                        releasePool.await();
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            await(blocked);

            final OperationInitializerJobScheduler scheduler = new OperationInitializerJobScheduler(pool);
            final Set<Thread> taskThreads = ConcurrentHashMap.newKeySet();
            final AtomicInteger followUps = new AtomicInteger();
            final AtomicReference<Thread> waitingThread = new AtomicReference<>();
            withTimeout(() -> {
                waitingThread.set(Thread.currentThread());
                final CompletableFuture<Void> done = new CompletableFuture<>();
                scheduler.iterateParallel(ExecutionContext.getContext(), null,
                        JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, 8,
                        (context, idx, nec) -> taskThreads.add(Thread.currentThread()),
                        () -> scheduler.submit(ExecutionContext.getContext(), () -> {
                            followUps.incrementAndGet();
                            done.complete(null);
                        }, null, done::completeExceptionally),
                        () -> {
                        },
                        done::completeExceptionally);
                scheduler.awaitCompletion(done);
            });

            assertThat(taskThreads).containsExactly(waitingThread.get());
            assertThat(followUps.get()).isEqualTo(1);
            assertThat(outstandingJobs(scheduler)).isZero();
        } finally {
            releasePool.countDown();
            pool.shutdown();
        }
    }

    /**
     * A job the waiting thread runs whose error handler fails cannot complete the future, so the wait fails it with
     * that failure rather than waiting for a completion that nothing will make.
     */
    @Test
    public void testFailingErrorHandlerOfAJobTheWaitRunsFailsTheWait() throws InterruptedException {
        final List<Runnable> queued = new ArrayList<>();
        final ExecutorJobScheduler scheduler = new ExecutorJobScheduler(queued::add, 2);
        final IllegalStateException handlerFailure = new IllegalStateException("handler failed");
        final CompletableFuture<Void> done = new CompletableFuture<>();
        scheduler.submit(null, () -> {
            throw new IllegalStateException("job failed");
        }, null, e -> {
            throw handlerFailure;
        });
        assertThat(queued).hasSize(1);

        withTimeout(() -> assertThatThrownBy(() -> scheduler.awaitCompletion(done))
                .isInstanceOf(ExecutionException.class)
                // the unit test fatal error reporter throws in place of the handler's failure, carrying it
                .hasCauseInstanceOf(FakeProcessEnvironment.FakeFatalException.class)
                .hasRootCause(handlerFailure));
        assertThat(outstandingJobs(scheduler)).isZero();
    }

    /**
     * Run {@code test} on a thread of its own, under this thread's execution context, so that a wait that never
     * finishes fails the test rather than hanging it.
     */
    private static void withTimeout(final ThrowingRunnable test) throws InterruptedException {
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                test.run();
            } catch (final Throwable t) {
                thrown.set(t);
            }
        }, "executor-job-scheduler-waiter");
        thread.setDaemon(true);
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(30));
        assertThat(thread.isAlive()).as("the waiting thread finished").isFalse();
        if (thrown.get() != null) {
            throw new AssertionError(thrown.get());
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
