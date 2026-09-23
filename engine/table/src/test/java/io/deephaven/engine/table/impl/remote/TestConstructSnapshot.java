//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.remote;

import io.deephaven.base.SleepUtil;
import io.deephaven.engine.context.ExecutionContext;
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

import static io.deephaven.engine.table.impl.SnapshotTestUtils.verifySnapshotBarrageMessage;
import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.stringCol;

public class TestConstructSnapshot extends RefreshingTableTestCase {

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
     * Regression test for DH-23460.
     *
     * <p>
     * A nested snapshot that falls back to a locked snapshot acquires the shared update graph lock, and (by design)
     * keeps it held until the outermost snapshot on the thread completes. If the enclosing concurrent attempt then
     * turns out to be inconsistent, the retry loop must fall back to a locked snapshot rather than attempting another
     * concurrent snapshot while holding the lock, and the lock must be released once the outermost snapshot exits.
     */
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
}
