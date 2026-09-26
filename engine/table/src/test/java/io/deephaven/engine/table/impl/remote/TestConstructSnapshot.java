//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.remote;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.SleepUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.exceptions.ColumnSnapshotUnsuccessfulException;
import io.deephaven.engine.exceptions.SnapshotUnsuccessfulException;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.NamingThreadFactory;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.deephaven.engine.table.impl.SnapshotTestUtils.verifySnapshotBarrageMessage;
import static org.junit.Assert.assertThrows;
import static io.deephaven.engine.table.impl.remote.ColumnSnapshotTestSupport.ATTEMPT_OVERRUN_MILLIS;
import static io.deephaven.engine.table.impl.remote.ColumnSnapshotTestSupport.INTERCEPTED_COLUMN_NAME;
import static io.deephaven.engine.table.impl.remote.ColumnSnapshotTestSupport.tableWithInterceptedColumn;
import static io.deephaven.engine.table.impl.remote.ColumnSnapshotTestSupport.withParallelColumnSnapshot;
import static io.deephaven.engine.table.impl.remote.ColumnSnapshotTestSupport.withSerialColumnSnapshot;
import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static org.junit.Assert.*;

public class TestConstructSnapshot extends RefreshingTableTestCase {

    @Test
    public void testClockChange() throws InterruptedException {
        final MutableLong changed = new MutableLong(0);
        final ConstructSnapshot.SnapshotControl control = new ConstructSnapshot.SnapshotControl() {

            @Override
            public Boolean usePreviousValues(long beforeClockValue) {
                // noinspection AutoBoxing
                return LogicalClock.getState(beforeClockValue) == LogicalClock.State.Updating;
            }

            @Override
            public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
                return true;
            }

            @Override
            public UpdateGraph getUpdateGraph() {
                return ExecutionContext.getContext().getUpdateGraph();
            }
        };
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final Runnable snapshot_test = () -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                ConstructSnapshot.callDataSnapshotFunction("snapshot test", control, (usePrev, beforeClock) -> {
                    SleepUtil.sleep(1000);
                    if (ConstructSnapshot.concurrentAttemptInconsistent()) {
                        changed.increment();
                    }
                    return true;
                });
            }
        };

        changed.set(0);
        final Thread t = new Thread(snapshot_test);
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
        t.start();
        t.join();
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();
        assertEquals(0, changed.get());

        changed.set(0);
        final Thread t2 = new Thread(snapshot_test);
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
        t2.start();
        SleepUtil.sleep(100);
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();
        t2.join();
        assertEquals(1, changed.get());
    }

    @Test
    public void testConstructBackplaneSnapshot() throws ExecutionException, InterruptedException {
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new NamingThreadFactory(TestConstructSnapshot.class, "TestConstructSnapshot Executor"));

        final QueryTable table = testRefreshingTable(i(1000).toTracking(), intCol("I", 10));
        final FunctionalColumn<Integer, String> plusOneColumn =
                new FunctionalColumn<>("I", Integer.class, "S2", String.class, (Integer i) -> Integer.toString(i + 1));
        final QueryTable functionalTable = (QueryTable) table.updateView(List.of(plusOneColumn));

        final BitSet oneBit = new BitSet();
        oneBit.set(0);
        final BitSet twoBits = new BitSet();
        twoBits.set(0, 2);

        try (final BarrageMessage initialSnapshot1 = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                "table", table, oneBit, RowSetFactory.fromRange(0, 10), null);
                final BarrageMessage funcSnapshot1 = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                        "functionalTable", functionalTable, twoBits, RowSetFactory.fromRange(0, 10), null)) {
            verifySnapshotBarrageMessage(initialSnapshot1, TableTools.newTable(intCol("I", 10)));
            verifySnapshotBarrageMessage(funcSnapshot1, TableTools.newTable(intCol("I", 10), stringCol("S2", "11")));
        }

        final ControlledUpdateGraph ug = ExecutionContext.getContext().getUpdateGraph().cast();

        ug.startCycleForUnitTests(false);
        addToTable(table, i(1000), intCol("I", 20));

        try (final BarrageMessage initialSnapshot2 = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                "table", table, oneBit, RowSetFactory.fromRange(0, 10), null);
                final BarrageMessage funcSnapshot2 = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                        "functionalTable", functionalTable, twoBits, RowSetFactory.fromRange(0, 10), null)) {
            table.notifyListeners(i(), i(), i(1000));
            ug.markSourcesRefreshedForUnitTests();

            // noinspection StatementWithEmptyBody
            while (ug.flushOneNotificationForUnitTests());

            try (final BarrageMessage initialSnapshot3 =
                    executor.submit(() -> ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                            "table", table, oneBit, RowSetFactory.fromRange(0, 10), null)).get();
                    final BarrageMessage funcSnapshot3 =
                            executor.submit(() -> ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                                    "functionalTable", functionalTable, twoBits, RowSetFactory.fromRange(0, 10), null))
                                    .get()) {
                ug.completeCycleForUnitTests();

                verifySnapshotBarrageMessage(initialSnapshot2, TableTools.newTable(intCol("I", 10)));
                verifySnapshotBarrageMessage(initialSnapshot3, TableTools.newTable(intCol("I", 20)));

                verifySnapshotBarrageMessage(funcSnapshot2,
                        TableTools.newTable(intCol("I", 10), stringCol("S2", "11")));
                verifySnapshotBarrageMessage(funcSnapshot3,
                        TableTools.newTable(intCol("I", 20), stringCol("S2", "21")));
            }
        }

        executor.shutdownNow();
    }

    private static final long TIMEOUT_SECONDS = 30;

    private static ConstructSnapshot.SnapshotControl makeCurrentValuesControl(@NotNull final UpdateGraph updateGraph) {
        return new ConstructSnapshot.SnapshotControl() {

            @Override
            public Boolean usePreviousValues(final long beforeClockValue) {
                // noinspection AutoBoxing
                return false;
            }

            @Override
            public boolean snapshotConsistent(final long currentClockValue, final boolean usingPreviousValues) {
                return true;
            }

            @Override
            public UpdateGraph getUpdateGraph() {
                return updateGraph;
            }
        };
    }

    /**
     * Make a snapshot function that refuses to run concurrently, forcing its snapshot to fall back to a locked snapshot
     * (which acquires the shared update graph lock if it is not already held).
     */
    private static ConstructSnapshot.SnapshotFunction makeLockForcingFunction(
            @NotNull final UpdateGraph updateGraph,
            @NotNull final AtomicInteger concurrentCalls,
            @NotNull final AtomicInteger lockedCalls) {
        return (final boolean usePrev, final long beforeClockValue) -> {
            if (!updateGraph.sharedLock().isHeldByCurrentThread()) {
                concurrentCalls.incrementAndGet();
                throw new ConstructSnapshot.NoSnapshotAllowedException();
            }
            lockedCalls.incrementAndGet();
            return true;
        };
    }

    /**
     * Assert that the (single) thread of {@code executor} does not hold the shared update graph lock, and that it can
     * still perform a concurrent snapshot.
     */
    private static void assertLockReleased(
            @NotNull final ExecutorService executor,
            @NotNull final ExecutionContext executionContext,
            @NotNull final UpdateGraph updateGraph,
            @NotNull final ConstructSnapshot.SnapshotControl control)
            throws InterruptedException, ExecutionException, TimeoutException {
        assertFalse(executor.submit(() -> updateGraph.sharedLock().isHeldByCurrentThread())
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        final AtomicBoolean subsequentSnapshotConcurrent = new AtomicBoolean();
        executor.submit(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                return ConstructSnapshot.callDataSnapshotFunction("subsequent", control,
                        (final boolean usePrev, final long beforeClockValue) -> {
                            subsequentSnapshotConcurrent.set(!updateGraph.sharedLock().isHeldByCurrentThread());
                            return true;
                        });
            }
        }).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertTrue(subsequentSnapshotConcurrent.get());
    }

    /**
     * Wait for {@code thread} to consume a pending interrupt. A parallel column snapshot clears the interrupt when its
     * wait for the column jobs throws, and restores it only once those jobs are done, so a cleared flag means the
     * interrupt has been seen while the jobs are still running.
     */
    private static void awaitInterruptConsumed(@NotNull final Thread thread) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (thread.isInterrupted()) {
            assertTrue("interrupt not consumed within " + TIMEOUT_SECONDS + "s", System.nanoTime() < deadline);
            Thread.sleep(1);
        }
    }

    /**
     * Regression test for DH-23460.
     *
     * <p>
     * A nested snapshot that falls back to a locked snapshot acquires the shared update graph lock, and (by design)
     * keeps it held until the outermost snapshot on the thread completes. If the enclosing concurrent attempt then
     * turns out to be inconsistent, the retry loop must fall back to a locked snapshot rather than attempting another
     * concurrent snapshot while holding the lock, and the lock must be released once the outermost snapshot exits.
     */
    @Test
    public void testNestedLockedSnapshotWithinInconsistentConcurrentAttempt()
            throws InterruptedException, ExecutionException, TimeoutException {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new NamingThreadFactory(TestConstructSnapshot.class, "TestConstructSnapshot Executor"));
        try {
            final ConstructSnapshot.SnapshotControl control = makeCurrentValuesControl(updateGraph);

            final CountDownLatch outerAttemptStarted = new CountDownLatch(1);
            final CountDownLatch cycleCompleted = new CountDownLatch(1);
            final AtomicInteger outerCalls = new AtomicInteger();
            final AtomicInteger innerConcurrentCalls = new AtomicInteger();
            final AtomicInteger innerLockedCalls = new AtomicInteger();

            final ConstructSnapshot.SnapshotFunction inner =
                    makeLockForcingFunction(updateGraph, innerConcurrentCalls, innerLockedCalls);

            final ConstructSnapshot.SnapshotFunction outer = (final boolean usePrev, final long beforeClockValue) -> {
                if (outerCalls.getAndIncrement() == 0) {
                    // First (concurrent) attempt: let the test thread run a full update cycle, so that this attempt
                    // will be inconsistent once the nested snapshot has completed.
                    outerAttemptStarted.countDown();
                    try {
                        cycleCompleted.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                ConstructSnapshot.callDataSnapshotFunction("inner", control, inner);
                return true;
            };

            final Future<Long> snapshotStep = executor.submit(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    return ConstructSnapshot.callDataSnapshotFunction("outer", control, outer);
                }
            });

            assertTrue(outerAttemptStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            updateGraph.startCycleForUnitTests();
            updateGraph.completeCycleForUnitTests();
            final long expectedStep = updateGraph.clock().currentStep();
            cycleCompleted.countDown();

            assertEquals(expectedStep, snapshotStep.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).longValue());
            // The outer snapshot makes one (inconsistent) concurrent attempt, then one locked attempt
            assertEquals(2, outerCalls.get());
            // The inner snapshot makes one concurrent attempt within the outer concurrent attempt, and then locked
            // attempts within each of the outer attempts
            assertEquals(1, innerConcurrentCalls.get());
            assertEquals(2, innerLockedCalls.get());

            assertLockReleased(executor, executionContext, updateGraph, control);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Companion to {@link #testNestedLockedSnapshotWithinInconsistentConcurrentAttempt()}: if the enclosing concurrent
     * attempt succeeds after a nested locked snapshot acquired the shared update graph lock, the lock must be released
     * when the outermost snapshot exits.
     */
    @Test
    public void testLockReleasedAfterSuccessfulConcurrentAttemptWithNestedLockedSnapshot()
            throws InterruptedException, ExecutionException, TimeoutException {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new NamingThreadFactory(TestConstructSnapshot.class, "TestConstructSnapshot Executor"));
        try {
            final ConstructSnapshot.SnapshotControl control = makeCurrentValuesControl(updateGraph);

            final AtomicInteger outerCalls = new AtomicInteger();
            final AtomicInteger innerConcurrentCalls = new AtomicInteger();
            final AtomicInteger innerLockedCalls = new AtomicInteger();
            final ConstructSnapshot.SnapshotFunction inner =
                    makeLockForcingFunction(updateGraph, innerConcurrentCalls, innerLockedCalls);

            final ConstructSnapshot.SnapshotFunction outer = (final boolean usePrev, final long beforeClockValue) -> {
                outerCalls.incrementAndGet();
                ConstructSnapshot.callDataSnapshotFunction("inner", control, inner);
                assertTrue(updateGraph.sharedLock().isHeldByCurrentThread());
                return true;
            };

            final long expectedStep = updateGraph.clock().currentStep();
            final Future<Long> snapshotStep = executor.submit(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    return ConstructSnapshot.callDataSnapshotFunction("outer", control, outer);
                }
            });
            assertEquals(expectedStep, snapshotStep.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).longValue());
            assertEquals(1, outerCalls.get());
            assertEquals(1, innerConcurrentCalls.get());
            assertEquals(1, innerLockedCalls.get());

            assertLockReleased(executor, executionContext, updateGraph, control);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Companion to {@link #testNestedLockedSnapshotWithinInconsistentConcurrentAttempt()}: if an exception escapes a
     * concurrent attempt after a nested locked snapshot acquired the shared update graph lock, the lock must still be
     * released when the outermost snapshot exits.
     */
    @Test
    public void testLockReleasedWhenExceptionEscapesConcurrentAttempt()
            throws InterruptedException, ExecutionException, TimeoutException {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new NamingThreadFactory(TestConstructSnapshot.class, "TestConstructSnapshot Executor"));
        try {
            final ConstructSnapshot.SnapshotControl control = makeCurrentValuesControl(updateGraph);

            final AtomicInteger innerConcurrentCalls = new AtomicInteger();
            final AtomicInteger innerLockedCalls = new AtomicInteger();
            final ConstructSnapshot.SnapshotFunction inner =
                    makeLockForcingFunction(updateGraph, innerConcurrentCalls, innerLockedCalls);

            final ConstructSnapshot.SnapshotFunction outer = (final boolean usePrev, final long beforeClockValue) -> {
                ConstructSnapshot.callDataSnapshotFunction("inner", control, inner);
                assertTrue(updateGraph.sharedLock().isHeldByCurrentThread());
                // SnapshotUnsuccessfulException is propagated from a concurrent attempt without any retry
                throw new SnapshotUnsuccessfulException("Deliberate failure after nested locked snapshot");
            };

            final Future<Long> snapshotStep = executor.submit(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    return ConstructSnapshot.callDataSnapshotFunction("outer", control, outer);
                }
            });
            try {
                snapshotStep.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                fail("Expected SnapshotUnsuccessfulException");
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof SnapshotUnsuccessfulException)) {
                    throw e;
                }
            }
            assertEquals(1, innerConcurrentCalls.get());
            assertEquals(1, innerLockedCalls.get());

            assertLockReleased(executor, executionContext, updateGraph, control);
        } finally {
            executor.shutdownNow();
        }
    }


    /**
     * Regression test for DH-23733.
     *
     * <p>
     * A column fill that throws must not leak the chunk it was filling — {@link RefreshingTableTestCase}'s teardown
     * fails the test if it does. The failure the caller sees wraps one thrown from the fill site on the thread that
     * read the column, which names that column and keeps the original exception as its cause.
     */
    @Test
    public void testParallelColumnSnapshotFailureNamesColumnAndReleasesChunk() {
        final RuntimeException failure = new IllegalStateException("Deliberate column fill failure");
        final QueryTable table = tableWithInterceptedColumn(() -> {
            throw failure;
        });
        withParallelColumnSnapshot(() -> {
            final ColumnSnapshotUnsuccessfulException thrown = assertThrows(
                    ColumnSnapshotUnsuccessfulException.class,
                    // Close the message on the path where the snapshot unexpectedly succeeds, so that a failure of
                    // this assertion is not compounded by a leak report from the chunks it holds.
                    () -> ConstructSnapshot.constructBackplaneSnapshot(this, table).close());
            final Throwable columnFailure = thrown.getCause();
            assertTrue(String.valueOf(columnFailure), columnFailure instanceof ColumnSnapshotUnsuccessfulException);
            assertTrue(columnFailure.getMessage(), columnFailure.getMessage().contains(INTERCEPTED_COLUMN_NAME));
            assertSame(failure, columnFailure.getCause());
        });
    }

    /**
     * Companion to {@link #testParallelColumnSnapshotFailureNamesColumnAndReleasesChunk()} for the serial path, which
     * names the failing column from the same fill site and releases the in-flight chunk.
     */
    @Test
    public void testSerialColumnSnapshotFailureNamesColumnAndReleasesChunk() {
        final RuntimeException failure = new IllegalStateException("Deliberate column fill failure");
        final QueryTable table = tableWithInterceptedColumn(() -> {
            throw failure;
        });
        withSerialColumnSnapshot(() -> {
            try (final BarrageMessage ignored = ConstructSnapshot.constructBackplaneSnapshot(this, table)) {
                fail("Expected ColumnSnapshotUnsuccessfulException");
            } catch (ColumnSnapshotUnsuccessfulException e) {
                assertTrue(e.getMessage(), e.getMessage().contains(INTERCEPTED_COLUMN_NAME));
                // Both fill branches share one catch, so only the message can say which of them ran.
                assertTrue(e.getMessage(), e.getMessage().contains("current values"));
                assertSame(failure, e.getCause());
            }
        });
    }

    /**
     * Regression test for DH-23733.
     *
     * <p>
     * Interrupting the thread waiting on a parallel column snapshot must not abandon the scheduled jobs: they are still
     * filling chunks into the {@link BarrageMessage}, which the caller closes as the failure propagates. The snapshot
     * waits for them to finish, then propagates a {@link CancellationException} with the interrupt restored.
     */
    @Test
    public void testParallelColumnSnapshotWaitsForJobsWhenInterrupted() throws InterruptedException {
        final CountDownLatch fillStarted = new CountDownLatch(1);
        final CountDownLatch releaseFill = new CountDownLatch(1);
        final AtomicBoolean fillCompleted = new AtomicBoolean();
        final QueryTable table = tableWithInterceptedColumn(() -> {
            fillStarted.countDown();
            try {
                releaseFill.await();
            } catch (InterruptedException e) {
                throw new UncheckedDeephavenException(e);
            }
            fillCompleted.set(true);
        });

        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final AtomicBoolean fillCompletedWhenThrown = new AtomicBoolean();
        final AtomicBoolean interruptRestored = new AtomicBoolean();
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final Thread snapshotThread = new Thread(() -> withParallelColumnSnapshot(() -> {
            try (final SafeCloseable ignored = executionContext.open();
                    final BarrageMessage ignored2 = ConstructSnapshot.constructBackplaneSnapshot(this, table)) {
                // Expected to throw
            } catch (Throwable t) {
                thrown.set(t);
                fillCompletedWhenThrown.set(fillCompleted.get());
                interruptRestored.set(Thread.interrupted());
            }
        }), "TestConstructSnapshot Interrupted Snapshot");
        snapshotThread.start();

        try {
            assertTrue(fillStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            snapshotThread.interrupt();
            awaitInterruptConsumed(snapshotThread);
            // Give an unfixed snapshot time to abandon the still-running job and return.
            snapshotThread.join(500);
            assertTrue("snapshotThread.isAlive()", snapshotThread.isAlive());
        } finally {
            // The intercepted fill occupies a common pool thread until this is released, so release it however this
            // test ends, and do not leave the non-daemon snapshot thread behind either.
            releaseFill.countDown();
            snapshotThread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            snapshotThread.interrupt();
        }
        assertFalse("snapshotThread.isAlive()", snapshotThread.isAlive());

        assertTrue(String.valueOf(thrown.get()), thrown.get() instanceof CancellationException);
        assertTrue("fillCompletedWhenThrown", fillCompletedWhenThrown.get());
        assertTrue("interruptRestored", interruptRestored.get());
    }

    /**
     * Regression test for DH-23733.
     *
     * <p>
     * A job that fails while the waiting thread is being interrupted has still failed. Cancellation is what the caller
     * asked for and is what it gets, but the column that could not be read is reported alongside it rather than
     * dropped.
     */
    @Test
    public void testInterruptedParallelColumnSnapshotReportsJobFailure() throws InterruptedException {
        final RuntimeException failure = new IllegalStateException("Deliberate column fill failure");
        final CountDownLatch fillStarted = new CountDownLatch(1);
        final CountDownLatch releaseFill = new CountDownLatch(1);
        final QueryTable table = tableWithInterceptedColumn(() -> {
            fillStarted.countDown();
            try {
                releaseFill.await();
            } catch (InterruptedException e) {
                throw new UncheckedDeephavenException(e);
            }
            throw failure;
        });

        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final Thread snapshotThread = new Thread(() -> withParallelColumnSnapshot(() -> {
            try (final SafeCloseable ignored = executionContext.open();
                    final BarrageMessage ignored2 = ConstructSnapshot.constructBackplaneSnapshot(this, table)) {
                // Expected to throw
            } catch (Throwable t) {
                thrown.set(t);
                Thread.interrupted();
            }
        }), "TestConstructSnapshot Interrupted Failing Snapshot");
        snapshotThread.start();

        try {
            assertTrue(fillStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            snapshotThread.interrupt();
            // A failure that completes the future before the waiting thread notices the interrupt would be reported by
            // CompletableFuture.get in place of the interrupt, so let the interrupt land first.
            awaitInterruptConsumed(snapshotThread);
        } finally {
            releaseFill.countDown();
            snapshotThread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            snapshotThread.interrupt();
        }
        assertFalse("snapshotThread.isAlive()", snapshotThread.isAlive());

        final Throwable cancellation = thrown.get();
        assertTrue(String.valueOf(cancellation), cancellation instanceof CancellationException);
        assertEquals("suppressed", 1, cancellation.getSuppressed().length);
        final Throwable jobFailure = cancellation.getSuppressed()[0];
        assertTrue(String.valueOf(jobFailure), jobFailure instanceof ColumnSnapshotUnsuccessfulException);
        final Throwable columnFailure = jobFailure.getCause();
        assertTrue(String.valueOf(columnFailure), columnFailure instanceof ColumnSnapshotUnsuccessfulException);
        assertTrue(columnFailure.getMessage(), columnFailure.getMessage().contains(INTERCEPTED_COLUMN_NAME));
        assertSame(failure, columnFailure.getCause());
    }

    /**
     * Regression test for DH-23733.
     *
     * <p>
     * The refreshing counterpart of {@link #testParallelColumnSnapshotWaitsForJobsWhenInterrupted()}. Waiting for the
     * jobs of a cancelled attempt can take longer than {@code ConstructSnapshot.maxConcurrentAttemptDurationMillis},
     * which is how long an attempt may run before the retry loop stops making concurrent attempts and falls back to a
     * locked one. A cancelled attempt must not reach that fallback: taking the update graph lock and snapshotting again
     * on behalf of a caller that has gone away is exactly what cancellation is asking us not to do.
     */
    @Test
    public void testCancelledRefreshingSnapshotIsNotRetriedUnderLock() throws InterruptedException {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final CountDownLatch fillStarted = new CountDownLatch(1);
        final CountDownLatch releaseFill = new CountDownLatch(1);
        final AtomicInteger interceptedFills = new AtomicInteger();
        final QueryTable table = tableWithInterceptedColumn(() -> {
            interceptedFills.incrementAndGet();
            fillStarted.countDown();
            try {
                releaseFill.await();
            } catch (InterruptedException e) {
                throw new UncheckedDeephavenException(e);
            }
        }, true);

        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final AtomicBoolean interruptRestored = new AtomicBoolean();
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final Thread snapshotThread = new Thread(() -> withParallelColumnSnapshot(() -> {
            try (final SafeCloseable ignored = executionContext.open();
                    final BarrageMessage ignored2 = ConstructSnapshot.constructBackplaneSnapshot(this, table)) {
                // Expected to throw
            } catch (Throwable t) {
                thrown.set(t);
                interruptRestored.set(Thread.interrupted());
            }
        }), "TestConstructSnapshot Cancelled Refreshing Snapshot");
        snapshotThread.start();

        try {
            assertTrue(fillStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            // Advance the clock while the attempt is parked, so that it is judged inconsistent.
            updateGraph.startCycleForUnitTests();
            updateGraph.completeCycleForUnitTests();
            snapshotThread.interrupt();
            // Hold the attempt open past the maximum concurrent attempt duration, so that the retry loop would go on
            // to a locked snapshot rather than to its retry delay.
            Thread.sleep(ATTEMPT_OVERRUN_MILLIS);
        } finally {
            releaseFill.countDown();
            snapshotThread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            snapshotThread.interrupt();
        }
        assertFalse("snapshotThread.isAlive()", snapshotThread.isAlive());

        assertTrue(String.valueOf(thrown.get()), thrown.get() instanceof CancellationException);
        assertTrue("interruptRestored", interruptRestored.get());
        // A second fill of this column would mean the cancelled attempt was followed by a locked one.
        assertEquals("interceptedFills", 1, interceptedFills.get());
    }

}
