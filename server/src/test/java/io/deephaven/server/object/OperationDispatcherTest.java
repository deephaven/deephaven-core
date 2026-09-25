//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.object;

import io.deephaven.server.object.ObjectServiceGrpcImpl.OperationDispatcher;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class OperationDispatcherTest {

    @Test
    public void runsOnSubmittingThread() {
        final OperationDispatcher dispatcher = new OperationDispatcher();
        final AtomicReference<Thread> ranOn = new AtomicReference<>();
        dispatcher.submit(() -> ranOn.set(Thread.currentThread()));
        assertThat(ranOn.get()).isSameAs(Thread.currentThread());
    }

    @Test
    public void runsOneAtATimeInOrder() {
        final OperationDispatcher dispatcher = new OperationDispatcher();
        final List<Integer> started = new ArrayList<>();

        dispatcher.submit(() -> started.add(0));
        for (int ii = 1; ii < 5; ++ii) {
            final int index = ii;
            dispatcher.submit(() -> started.add(index));
            // the first operation still holds the dispatcher, so nothing else may have started
            assertThat(started).containsExactly(0);
        }

        for (int ii = 1; ii < 5; ++ii) {
            dispatcher.operationComplete();
            assertThat(started).hasSize(ii + 1);
        }
        assertThat(started).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    public void closedDispatcherStartsNothing() {
        final OperationDispatcher dispatcher = new OperationDispatcher();
        final AtomicInteger runs = new AtomicInteger();

        dispatcher.submit(runs::incrementAndGet);
        dispatcher.close();
        assertThat(dispatcher.isClosed()).isTrue();

        dispatcher.operationComplete();
        dispatcher.submit(runs::incrementAndGet);
        assertThat(runs.get()).isEqualTo(1);
    }

    /**
     * Submits operations from several threads while others complete them, which is the interleaving that occurs when
     * gRPC delivers a message while the export scheduler is finishing the previous one. Every submitted operation must
     * run exactly once, never concurrently with another, and never be stranded behind a dispatcher that nobody
     * re-checks; submitting must not throw.
     */
    @Test
    public void concurrentSubmitAndCompleteStrandsNothing() throws Exception {
        final int submitters = 4;
        final int operationsPerSubmitter = 2_000;
        final int rounds = 50;
        final int total = submitters * operationsPerSubmitter;

        final ExecutorService completers = Executors.newFixedThreadPool(4);
        try {
            for (int round = 0; round < rounds; ++round) {
                final OperationDispatcher dispatcher = new OperationDispatcher();
                final CountDownLatch allRan = new CountDownLatch(total);
                final AtomicInteger inFlight = new AtomicInteger();
                final AtomicReference<Throwable> failure = new AtomicReference<>();

                final Runnable operation = () -> {
                    if (inFlight.incrementAndGet() != 1) {
                        failure.compareAndSet(null, new AssertionError("operations ran concurrently"));
                    }
                    inFlight.decrementAndGet();
                    // complete off-thread, racing whatever the submitting threads are doing; the completion must be
                    // accepted before the latch drops, or the executor can be shut down out from under it
                    completers.execute(dispatcher::operationComplete);
                    allRan.countDown();
                };

                final CountDownLatch start = new CountDownLatch(1);
                final List<Thread> threads = new ArrayList<>(submitters);
                for (int ii = 0; ii < submitters; ++ii) {
                    final Thread thread = new Thread(() -> {
                        try {
                            start.await();
                            for (int jj = 0; jj < operationsPerSubmitter; ++jj) {
                                dispatcher.submit(operation);
                            }
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                    }, "submitter-" + ii);
                    threads.add(thread);
                    thread.start();
                }
                start.countDown();
                for (final Thread thread : threads) {
                    thread.join();
                }

                assertThat(allRan.await(30, TimeUnit.SECONDS))
                        .withFailMessage("round %d stranded %d operations", round, allRan.getCount())
                        .isTrue();
                // checked after the latch so that violations recorded by the operations themselves are seen
                if (failure.get() != null) {
                    throw new AssertionError("round " + round + " failed", failure.get());
                }
            }
        } finally {
            completers.shutdownNow();
        }
    }
}
