//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable;
import io.deephaven.engine.util.input.InputTableStatusListener;
import io.deephaven.engine.util.input.InputTableUpdater;
import io.deephaven.engine.testutil.*;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.concurrent.*;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class TestConcurrentInstantiationWhereDynamic extends TestConcurrentInstantiationBase {
    private static final long BLOCKED_TIMEOUT_MILLIS = 2_000;

    /**
     * Operations that {@link #assertAllTimeOut} left running because they were blocked on an unsatisfied dependency.
     * They must finish before the update graph is torn down, or a worker can still hold the update graph lock when the
     * next test begins.
     */
    private final List<Future<?>> blockedOperations = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void tearDown() throws Exception {
        // Tear the graph and the pools down even when a blocked operation failed, so that the failure is reported
        // against this test rather than contaminating the next one.
        try {
            awaitBlockedOperations();
        } finally {
            super.tearDown();
        }
    }

    /**
     * Assert that every submitted operation blocks, rather than completing, because a dependency is not yet satisfied.
     * <p>
     * All futures must already be submitted: waiting on the first inline would throw {@link TimeoutException} and the
     * later operations would never run, leaving them silently untested. Because they were submitted together, they
     * share one {@link #BLOCKED_TIMEOUT_MILLIS} deadline; once the first has outlasted it, the rest are checked at
     * once.
     */
    private void assertAllTimeOut(final Future<?>... futures)
            throws InterruptedException, ExecutionException {
        // Each of these is still running, and must be awaited before the update graph is torn down. Register them all
        // before waiting on any: if an early one unexpectedly completes, fail() exits the loop below, and an
        // unregistered later one would be left running into the next test.
        blockedOperations.addAll(Arrays.asList(futures));
        final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BLOCKED_TIMEOUT_MILLIS);
        for (int fi = 0; fi < futures.length; ++fi) {
            try {
                futures[fi].get(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
                fail("Expected operation " + fi + " to time out waiting for dependencies");
            } catch (final TimeoutException ignored) {
            }
        }
    }

    /**
     * Let every operation that was left blocked finish, now that the cycle it was waiting on has completed. Without
     * this, a worker can still be inside a table operation, holding the update graph lock, when the test ends.
     * <p>
     * Each of these operations must complete successfully once its dependency is satisfied, so a failure or a further
     * timeout is reported rather than swallowed: an operation that never unblocks, or that unblocks and then throws, is
     * exactly what these tests exist to catch. Every future is still awaited, and a timed out one cancelled, before the
     * first failure is reported, so a reported failure cannot leave a worker running.
     */
    private void awaitBlockedOperations() {
        final List<Future<?>> toAwait;
        synchronized (blockedOperations) {
            toAwait = new ArrayList<>(blockedOperations);
            blockedOperations.clear();
        }
        AssertionError firstFailure = null;
        for (int fi = 0; fi < toAwait.size(); ++fi) {
            final Future<?> future = toAwait.get(fi);
            AssertionError failure = null;
            try {
                future.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
            } catch (final ExecutionException e) {
                failure = new AssertionError(
                        "Blocked operation " + fi + " failed after its dependency was satisfied", e.getCause());
            } catch (final TimeoutException e) {
                future.cancel(true);
                failure = new AssertionError(
                        "Blocked operation " + fi + " never completed after its dependency was satisfied", e);
            } catch (final InterruptedException e) {
                future.cancel(true);
                Thread.currentThread().interrupt();
                failure = new AssertionError("Interrupted awaiting blocked operation " + fi, e);
            }
            if (failure != null && firstFailure == null) {
                firstFailure = failure;
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    @Test
    public void testWhereDynamic() throws ExecutionException, InterruptedException, TimeoutException {
        testWhereDynamicInternal(false, false);
        testWhereNotInDynamicInternal(false, false);
        testWhereDynamicInternalNotificationOrder(true, false, false);
        testWhereDynamicInternalNotificationOrder(false, false, false);
        testWhereDynamicInternalStaticSource(false, false);
    }

    @Test
    public void testWhereDynamicIndexedSource() throws ExecutionException, InterruptedException, TimeoutException {
        testWhereDynamicInternal(true, false);
        testWhereNotInDynamicInternal(true, false);
        testWhereDynamicInternalNotificationOrder(true, true, false);
        testWhereDynamicInternalNotificationOrder(false, true, false);
        testWhereDynamicInternalStaticSource(true, false);
    }

    @Test
    public void testWhereDynamicIndexedSet() throws ExecutionException, InterruptedException, TimeoutException {
        testWhereDynamicInternal(false, true);
        testWhereNotInDynamicInternal(false, true);
        testWhereDynamicInternalNotificationOrder(true, false, true);
        testWhereDynamicInternalNotificationOrder(false, false, true);
        testWhereDynamicInternalStaticSource(false, true);
    }

    @Test
    public void testWhereDynamicIndexedBoth() throws ExecutionException, InterruptedException, TimeoutException {
        testWhereDynamicInternal(true, true);
        testWhereNotInDynamicInternal(true, true);
        testWhereDynamicInternalNotificationOrder(true, true, true);
        testWhereDynamicInternalNotificationOrder(false, true, true);
        testWhereDynamicInternalStaticSource(true, true);
    }

    private void testWhereDynamicInternal(final boolean sourceIndexed, final boolean setIndexed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6, 8, 10).toTracking(),
                col("x", 1, 2, 3, 4, 5), col("y", "a", "b", "c", "d", "e"), col("z", true, false, true, false, true));
        if (sourceIndexed) {
            DataIndexer.getOrCreateDataIndex(table, "z");
        }
        final QueryTable whereTable = TstUtils.testRefreshingTable(i(0).toTracking(), col("z", true));
        if (setIndexed) {
            DataIndexer.getOrCreateDataIndex(whereTable, "z");
        }

        // Create a dynamic where filter on the main thread.
        final DynamicWhereFilter filter =
                new DynamicWhereFilter(whereTable, true, MatchPairFactory.getExpressions("z"));

        updateGraph.startCycleForUnitTests(false);

        // Expected result of the filters before any mods to the table.
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 6, 10).toTracking(),
                col("x", 1, 3, 5), col("y", "a", "c", "e"), col("z", true, true, true));

        final Table table1 = dualPool.submit(() -> table.where(filter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(tableStart, table1);

        // Add rows to the main table.
        TstUtils.addToTable(table, i(2, 3), col("x", 1, 4), col("y", "a", "d"), col("z", false, true));
        assertTableEquals(tableStart, prevTable(table1));

        final Table table2 = dualPool.submit(() -> table.where("z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(tableStart, prevTable(table2));

        final Table table3 = dualPool.submit(() -> {
            // Create a dynamic where filter on a worker thread.
            final DynamicWhereFilter filter3 =
                    new DynamicWhereFilter(whereTable, true, MatchPairFactory.getExpressions("z"));
            return table.where(filter3);
        }).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(tableStart, prevTable(table3));

        // Notify the children of the added / modified rows
        table.notifyListeners(i(3), i(), i(2));
        updateGraph.markSourcesRefreshedForUnitTests();

        updateGraph.completeCycleForUnitTests();

        // Expected result of the filters after the cycle ends
        final Table testUpdate = TstUtils.testRefreshingTable(i(3, 6, 10).toTracking(),
                col("x", 4, 3, 5), col("y", "d", "c", "e"), col("z", true, true, true));

        TstUtils.assertTableEquals(testUpdate, table1);
        TstUtils.assertTableEquals(table2, table1);
        TstUtils.assertTableEquals(table3, table2);
    }

    /**
     * The exclusion-mode mirror of {@link #testWhereDynamicInternal}. Exclusion takes different filtering and recompute
     * branches, and {@code whereNotIn} is declared concurrent alongside {@code whereIn}, so it needs its own current
     * and previous snapshot coverage over indexed and unindexed inputs.
     */
    private void testWhereNotInDynamicInternal(final boolean sourceIndexed, final boolean setIndexed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6, 8, 10).toTracking(),
                col("x", 1, 2, 3, 4, 5), col("y", "a", "b", "c", "d", "e"), col("z", true, false, true, false, true));
        if (sourceIndexed) {
            DataIndexer.getOrCreateDataIndex(table, "z");
        }
        final QueryTable whereTable = TstUtils.testRefreshingTable(i(0).toTracking(), col("z", true));
        if (setIndexed) {
            DataIndexer.getOrCreateDataIndex(whereTable, "z");
        }

        // Exclusion mode: keep the rows whose key is absent from the set table.
        final DynamicWhereFilter filter =
                new DynamicWhereFilter(whereTable, false, MatchPairFactory.getExpressions("z"));

        updateGraph.startCycleForUnitTests(false);

        // Expected result of the filters before any mods to the table.
        final Table tableStart = TstUtils.testRefreshingTable(i(4, 8).toTracking(),
                col("x", 2, 4), col("y", "b", "d"), col("z", false, false));

        final Table table1 = dualPool.submit(() -> table.where(filter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(tableStart, table1);

        // Add rows to the main table.
        TstUtils.addToTable(table, i(2, 3), col("x", 1, 4), col("y", "a", "d"), col("z", false, true));
        assertTableEquals(tableStart, prevTable(table1));

        final Table table2 = dualPool.submit(() -> table.where("!z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(tableStart, prevTable(table2));

        // Build the exclusion filter through the public API on a worker thread.
        final Table table3 =
                dualPool.submit(() -> table.whereNotIn(whereTable, "z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(tableStart, prevTable(table3));

        // Notify the children of the added / modified rows
        table.notifyListeners(i(3), i(), i(2));
        updateGraph.markSourcesRefreshedForUnitTests();

        updateGraph.completeCycleForUnitTests();

        // Expected result of the filters after the cycle ends
        final Table testUpdate = TstUtils.testRefreshingTable(i(2, 4, 8).toTracking(),
                col("x", 1, 2, 4), col("y", "a", "b", "d"), col("z", false, false, false));

        TstUtils.assertTableEquals(testUpdate, table1);
        TstUtils.assertTableEquals(table2, table1);
        TstUtils.assertTableEquals(table3, table2);
    }

    /**
     * Exercise a cycle in which the source and the set table are both notified, in either order. The order changes the
     * downstream notification sequence significantly, so both are covered.
     *
     * @param sourceFirst Whether to notify the source table before the set table
     */
    private void testWhereDynamicInternalNotificationOrder(
            final boolean sourceFirst, final boolean sourceIndexed, final boolean setIndexed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable source = TstUtils.testRefreshingTable(i(2, 4, 6, 8, 10).toTracking(),
                col("x", 1, 2, 3, 4, 5), col("y", "a", "b", "c", "d", "e"), col("z", true, false, true, false, true));
        if (sourceIndexed) {
            DataIndexer.getOrCreateDataIndex(source, "z");
        }
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), col("z", true));
        if (setIndexed) {
            DataIndexer.getOrCreateDataIndex(setTable, "z").table();
        }

        // Expected result of the filters before any mods to the tables.
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 6, 10).toTracking(),
                col("x", 1, 3, 5), col("y", "a", "c", "e"), col("z", true, true, true));

        updateGraph.startCycleForUnitTests(false);

        // This creates a set kernel from the prev setTable
        final DynamicWhereFilter filter =
                new DynamicWhereFilter(setTable, true, MatchPairFactory.getExpressions("z"));

        // This call succeeds because all tables are in the same (prev) state
        final Table prevFiltered1 =
                largePool.submit(() -> source.where(filter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table prevFiltered2 =
                largePool.submit(() -> source.whereIn(setTable, "z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(tableStart, prevFiltered1);
        assertTableEquals(tableStart, prevFiltered2);

        // Make changes to the source and set tables.
        TstUtils.addToTable(setTable, i(1), col("z", false));
        TstUtils.addToTable(source, i(2, 3), col("x", 1, 4), col("y", "a", "d"), col("z", false, true));

        // NOTE: the notification order changes the downstream notifications significantly.
        if (sourceFirst) {
            source.notifyListeners(i(3), i(), i(2));
            setTable.notifyListeners(i(1), i(), i());
        } else {
            setTable.notifyListeners(i(1), i(), i());
            source.notifyListeners(i(3), i(), i(2));
        }

        updateGraph.markSourcesRefreshedForUnitTests();

        assertTrue(source.satisfied(updateGraph.clock().currentStep()));
        assertTrue(setTable.satisfied(updateGraph.clock().currentStep()));
        assertFalse(filter.satisfied(updateGraph.clock().currentStep()));

        // Submit both before waiting: waiting on the first inline would throw, and the second operation would
        // never run.
        final Future<Table> copiedFilterResult = largePool.submit(() -> source.where(filter.copy()));
        final Future<Table> freshWhereInResult = largePool.submit(() -> source.whereIn(setTable, "z"));

        // A copy of the existing filter shares a set table that has not yet caught up, so it must wait.
        assertAllTimeOut(copiedFilterResult);

        if (sourceIndexed || setIndexed) {
            // A data index table has not caught up either, so a freshly built whereIn must wait as well.
            assertAllTimeOut(freshWhereInResult);
        } else {
            // With no data index involved, a freshly built whereIn takes its own consistent snapshot of the
            // current state and completes immediately rather than waiting. That is the concurrency this change
            // is for, so assert the result rather than a timeout.
            assertTableEquals(source, freshWhereInResult.get(TIMEOUT_LENGTH, TIMEOUT_UNIT));
        }

        // The filter is still not satisfied.
        assertFalse(filter.satisfied(updateGraph.clock().currentStep()));

        // If the source has an index, let it catch up
        if (sourceIndexed) {
            final Table indexTable = DataIndexer.getDataIndex(source, "z").table();
            assertFalse(indexTable.satisfied(updateGraph.clock().currentStep()));

            assertAllTimeOut(
                    largePool.submit(() -> source.where(filter.copy())),
                    largePool.submit(() -> source.whereIn(setTable, "z")));

            while (!indexTable.satisfied(updateGraph.clock().currentStep())) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }
        }

        // Allow the filter to get to current.
        while (!filter.satisfied(updateGraph.clock().currentStep())) {
            assertTrue(updateGraph.flushOneNotificationForUnitTests());
        }

        // This succeeds because source and setTable are now both satisfied
        final Table finalFiltered1 =
                largePool.submit(() -> source.where(filter.copy())).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table finalFiltered2 =
                largePool.submit(() -> source.whereIn(setTable, "z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTrue(finalFiltered1.satisfied(updateGraph.clock().currentStep()));
        assertTrue(finalFiltered2.satisfied(updateGraph.clock().currentStep()));

        assertTableEquals(source, finalFiltered1);
        assertTableEquals(source, finalFiltered2);

        updateGraph.completeCycleForUnitTests();

        // The operations left blocked above unblock now that the cycle is over. Wait for them here, so that they finish
        // against this sub-test's tables rather than inside the next sub-test's cycle.
        awaitBlockedOperations();

        // Now all the tables created in the cycle are correct
        assertTableEquals(source, prevFiltered1);
        assertTableEquals(source, prevFiltered2);
        assertTableEquals(source, finalFiltered1);
        assertTableEquals(source, finalFiltered2);
    }

    private void testWhereDynamicInternalStaticSource(final boolean sourceIndexed, final boolean setIndexed)
            throws ExecutionException, InterruptedException, TimeoutException {
        // Source table is static, set table is refreshing
        final QueryTable source = TstUtils.testTable(i(2, 4, 6, 8, 10).toTracking(),
                col("x", 1, 2, 3, 4, 5), col("y", "a", "b", "c", "d", "e"), col("z", true, false, true, false, true));
        if (sourceIndexed) {
            DataIndexer.getOrCreateDataIndex(source, "z");
        }
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), col("z", true));
        if (setIndexed) {
            DataIndexer.getOrCreateDataIndex(setTable, "z").table();
        }

        // Expected result of the filters before any mods to the tables.
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 6, 10).toTracking(),
                col("x", 1, 3, 5), col("y", "a", "c", "e"), col("z", true, true, true));

        updateGraph.startCycleForUnitTests(false);

        // This creates a set kernel from the prev setTable
        final DynamicWhereFilter filter =
                new DynamicWhereFilter(setTable, true, MatchPairFactory.getExpressions("z"));

        // Static source is always satisfied.
        assertTrue(source.satisfied(updateGraph.clock().currentStep()));

        // The set is not yet satisfied and nothing else can be, so each operation reads the set as it stands and
        // completes at once, then catches up when the set ticks below.
        final Table earlyFiltered1 =
                largePool.submit(() -> source.where(filter.copy())).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table earlyFiltered2 =
                largePool.submit(() -> source.whereIn(setTable, "z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(tableStart, earlyFiltered1);
        assertTableEquals(tableStart, earlyFiltered2);

        // Make changes to the set tables.
        TstUtils.addToTable(setTable, i(1), col("z", false));
        setTable.notifyListeners(i(1), i(), i());

        updateGraph.markSourcesRefreshedForUnitTests();

        assertTrue(source.satisfied(updateGraph.clock().currentStep()));
        assertTrue(setTable.satisfied(updateGraph.clock().currentStep()));
        assertFalse(filter.satisfied(updateGraph.clock().currentStep()));

        // Allow filter to get to current.
        assertFalse(filter.satisfied(updateGraph.clock().currentStep()));
        while (!filter.satisfied(updateGraph.clock().currentStep())) {
            assertTrue(updateGraph.flushOneNotificationForUnitTests());
        }

        // This succeeds because source and setTable are now both satisfied
        final Table filtered1 =
                largePool.submit(() -> source.where(filter.copy())).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table filtered2 =
                largePool.submit(() -> source.whereIn(setTable, "z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTrue(filtered1.satisfied(updateGraph.clock().currentStep()));
        assertTrue(filtered2.satisfied(updateGraph.clock().currentStep()));

        assertTableEquals(source, filtered1);
        assertTableEquals(source, filtered2);

        updateGraph.completeCycleForUnitTests();

        // Now all the tables created in the cycle are correct, including those built before the set ticked
        assertTableEquals(source, earlyFiltered1);
        assertTableEquals(source, earlyFiltered2);
        assertTableEquals(source, filtered1);
        assertTableEquals(source, filtered2);
    }

    /** Which data index, if any, the source table carries for the filter's key columns. */
    private enum SourceIndex {
        NONE, PARTIAL, FULL
    }

    /** Surfaces input table write failures, which the default listener would only log. */
    private static final InputTableStatusListener FAIL_ON_ERROR = new InputTableStatusListener() {
        @Override
        public void onError(final Throwable t) {
            throw new RuntimeException("Input table update failed", t);
        }
    };

    /**
     * Repro for DH-23539: a {@code where} using a {@link DynamicWhereFilter} backed by a refreshing input table,
     * applied to a live table from a thread that holds neither update graph lock and is not marked serial-safe (a gRPC
     * request thread resolving a ticket through an ACL transform). The source table has no data index for the filter's
     * key columns, so the filter searches for a partial index, which used to assert the lock and throw
     * {@code IllegalStateException}.
     */
    @Test
    public void testWhereDynamicInputTableWithoutLock() throws Exception {
        testWhereDynamicInputTableWithoutLockInternal(SourceIndex.NONE);
    }

    @Test
    public void testWhereDynamicInputTableWithoutLockPartialIndex() throws Exception {
        testWhereDynamicInputTableWithoutLockInternal(SourceIndex.PARTIAL);
    }

    @Test
    public void testWhereDynamicInputTableWithoutLockFullIndex() throws Exception {
        testWhereDynamicInputTableWithoutLockInternal(SourceIndex.FULL);
    }

    private void testWhereDynamicInputTableWithoutLockInternal(final SourceIndex sourceIndex) throws Exception {
        // A live source table.
        final QueryTable source = TstUtils.testRefreshingTable(i(2, 4, 6, 8, 10).toTracking(),
                stringCol("Sym", "A", "B", "C", "D", "E"),
                intCol("Acct", 1, 2, 1, 2, 1),
                intCol("Val", 10, 20, 30, 40, 50));
        switch (sourceIndex) {
            case PARTIAL:
                // A strict subset of the filter's key columns, so the filter must search for a partial index.
                DataIndexer.getOrCreateDataIndex(source, "Sym");
                break;
            case FULL:
                DataIndexer.getOrCreateDataIndex(source, "Sym", "Acct");
                break;
            case NONE:
                break;
        }

        // An ACL-style set table: a refreshing, keyed input table.
        final KeyedArrayBackedInputTable setTable = KeyedArrayBackedInputTable.make(
                newTable(stringCol("Sym", "A", "C"), intCol("Acct", 1, 1)), "Sym", "Acct");
        final InputTableUpdater setUpdater = InputTableUpdater.from(setTable);

        // The filter is built ahead of time, on a thread where serial operations are permitted, exactly as the ACL
        // transformer builds its filters.
        final DynamicWhereFilter filter =
                new DynamicWhereFilter(setTable, true, MatchPairFactory.getExpressions("Sym", "Acct"));

        // The worker must genuinely hold no lock and have no serial-operation permission, like a gRPC thread.
        assertFalse(pool.submit(() -> updateGraph.serialTableOperationsSafe()
                || updateGraph.sharedLock().isHeldByCurrentThread()
                || updateGraph.exclusiveLock().isHeldByCurrentThread()
                || updateGraph.currentThreadProcessesUpdates()).get(TIMEOUT_LENGTH, TIMEOUT_UNIT));

        // The update graph is idle, and the operations run on a worker thread without any lock.
        final Table filtered = pool.submit(() -> source.where(filter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        // The same through the public API, with the filter also built on the worker thread.
        final Table filteredWhereIn =
                pool.submit(() -> source.whereIn(setTable, "Sym", "Acct")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        final Table expectedStart = newTable(stringCol("Sym", "A", "C"), intCol("Acct", 1, 1), intCol("Val", 10, 30));
        assertTableEquals(expectedStart, filtered);
        assertTableEquals(expectedStart, filteredWhereIn);
        assertTrue(filtered.isRefreshing());
        assertTrue(filteredWhereIn.isRefreshing());

        // The results must then track the set table: add a key to the input table.
        setUpdater.addAsync(newTable(stringCol("Sym", "B"), intCol("Acct", 2)), FAIL_ON_ERROR);
        updateGraph.runWithinUnitTestCycle(setTable::run);
        final Table expectedAfterSetAdd = newTable(
                stringCol("Sym", "A", "B", "C"), intCol("Acct", 1, 2, 1), intCol("Val", 10, 20, 30));
        assertTableEquals(expectedAfterSetAdd, filtered);
        assertTableEquals(expectedAfterSetAdd, filteredWhereIn);

        // And the source table: add a row with a key in the set, and one with a key not in the set.
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(12, 14),
                    stringCol("Sym", "A", "A"), intCol("Acct", 1, 2), intCol("Val", 60, 70));
            source.notifyListeners(i(12, 14), i(), i());
        });
        final Table expectedAfterSourceAdd = newTable(
                stringCol("Sym", "A", "B", "C", "A"), intCol("Acct", 1, 2, 1, 1), intCol("Val", 10, 20, 30, 60));
        assertTableEquals(expectedAfterSourceAdd, filtered);
        assertTableEquals(expectedAfterSourceAdd, filteredWhereIn);

        // Remove a key from the input table.
        setUpdater.deleteAsync(newTable(stringCol("Sym", "A"), intCol("Acct", 1)), FAIL_ON_ERROR);
        updateGraph.runWithinUnitTestCycle(setTable::run);
        final Table expectedAfterSetDelete = newTable(
                stringCol("Sym", "B", "C"), intCol("Acct", 2, 1), intCol("Val", 20, 30));
        assertTableEquals(expectedAfterSetDelete, filtered);
        assertTableEquals(expectedAfterSetDelete, filteredWhereIn);
    }
}
