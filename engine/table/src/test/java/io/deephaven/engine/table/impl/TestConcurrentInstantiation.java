//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.base.SleepUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.hierarchical.TreeTableFilter;
import io.deephaven.engine.table.impl.hierarchical.TreeTableImpl;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class TestConcurrentInstantiation extends TestConcurrentInstantiationBase {
    @Test
    public void testTreeTableFilter() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable source = TstUtils.testRefreshingTable(
                RowSetFactory.flat(10).toTracking(),
                col("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                col("Parent", NULL_INT, NULL_INT, 1, 1, 2, 3, 5, 5, 3, 2));
        final TreeTable treed = source.tree("Sentinel", "Parent");
        final Callable<Table> callable =
                () -> (QueryTable) treed.getSource().apply(new TreeTableFilter.Operator((TreeTableImpl) treed,
                        WhereFilterFactory.getExpressions("Sentinel in 4, 6, 9, 11, 12, 13, 14, 15")));

        updateGraph.startCycleForUnitTests(false);

        final Table rawSorted = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        TableTools.show(rawSorted);

        assertArrayEquals(new int[] {1, 3, 4, 6, 9}, ColumnVectors.ofInt(rawSorted, "Sentinel").toArray());

        TstUtils.addToTable(source,
                i(10),
                col("Sentinel", 11),
                col("Parent", 2));
        final Table table2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(rawSorted, table2);

        source.notifyListeners(i(10), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Future<Table> future3 = pool.submit(callable);
        assertTableEquals(rawSorted, table2);

        updateGraph.completeCycleForUnitTests();
        final Table table3 = future3.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(rawSorted, table2);
        assertTableEquals(table2, table3);

        updateGraph.startCycleForUnitTests(false);
        TstUtils.addToTable(source,
                i(11),
                col("Sentinel", 12),
                col("Parent", 10));

        final Table table4 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(rawSorted, table2);
        assertTableEquals(table2, table3);
        assertTableEquals(table3, table4);

        source.notifyListeners(i(11), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();
        updateGraph.completeCycleForUnitTests();

        assertArrayEquals(
                new int[] {1, 2, 3, 4, 6, 9, 10, 11, 12},
                ColumnVectors.ofInt(rawSorted, "Sentinel").toArray());
        assertTableEquals(rawSorted, table2);
        assertTableEquals(table2, table3);
        assertTableEquals(table3, table4);
    }

    @Test
    public void testFlatten() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"));

        updateGraph.startCycleForUnitTests(false);

        final Table flat = pool.submit(table::flatten).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(flat, table);
        assertTableEquals(flat, tableStart);

        TstUtils.addToTable(table, i(3), col("x", 4), col("y", "d"));

        final Table flat2 = pool.submit(table::flatten).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(prevTable(flat), tableStart);
        TstUtils.assertTableEquals(prevTable(flat2), tableStart);

        table.notifyListeners(i(3), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table flat3 = pool.submit(table::flatten).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(prevTable(flat), tableStart);
        TstUtils.assertTableEquals(prevTable(flat2), tableStart);

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(table, flat);
        TstUtils.assertTableEquals(table, flat2);
        TstUtils.assertTableEquals(table, flat3);
    }

    @Test
    public void testUngroupRollingGroup() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("Sym", "a", "b", "a"), intCol("x", 1, 2, 3));
        final Table grouped = table.updateBy(UpdateByOperation.RollingGroup(2, 0, "x"), "Sym");

        final Table expect1 = TstUtils.testTable(col("Sym", "a", "b", "a"), intCol("x", 1, 2, 3))
                .updateBy(UpdateByOperation.RollingGroup(2, 0, "x"), "Sym").ungroup("x");
        final Table expect2 = TstUtils.testTable(col("Sym", "a", "b", "b", "a", "b"), intCol("x", 1, 4, 2, 3, 5))
                .updateBy(UpdateByOperation.RollingGroup(2, 0, "x"), "Sym").ungroup("x");

        final Callable<Table> callable = () -> grouped.ungroup("x");

        updateGraph.startCycleForUnitTests(false);

        final Table ungroup1 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(expect1, ungroup1);

        TstUtils.addToTable(table, i(3, 8), col("Sym", "b", "b"), intCol("x", 4, 5));
        table.notifyListeners(i(3, 8), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        // the rolling group has not yet processed the update, so instantiation must use previous values
        final Table ungroup2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(expect1, prevTable(ungroup1));
        assertTableEquals(expect1, prevTable(ungroup2));

        updateGraph.completeCycleForUnitTests();

        assertTableEquals(expect2, ungroup1);
        assertTableEquals(expect2, ungroup2);
    }

    @Test
    public void testUngroupRollingGroupTimed() throws ExecutionException, InterruptedException, TimeoutException {
        final Instant baseTime = DateTimeUtils.parseInstant("2025-01-01T09:30:00 NY");
        final Duration rev = Duration.ofSeconds(15);
        final Duration fwd = Duration.ZERO;

        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("Sym", "a", "b", "a"),
                instantCol("ts", baseTime, baseTime.plusSeconds(10), baseTime.plusSeconds(20)),
                intCol("x", 1, 2, 3));
        final Table grouped = table.updateBy(UpdateByOperation.RollingGroup("ts", rev, fwd, "x"), "Sym");

        final Table expect1 = TstUtils.testTable(
                col("Sym", "a", "b", "a"),
                instantCol("ts", baseTime, baseTime.plusSeconds(10), baseTime.plusSeconds(20)),
                intCol("x", 1, 2, 3))
                .updateBy(UpdateByOperation.RollingGroup("ts", rev, fwd, "x"), "Sym").ungroup("x");
        final Table expect2 = TstUtils.testTable(
                col("Sym", "a", "b", "b", "a", "b"),
                instantCol("ts", baseTime, baseTime.plusSeconds(5), baseTime.plusSeconds(10),
                        baseTime.plusSeconds(20), baseTime.plusSeconds(30)),
                intCol("x", 1, 4, 2, 3, 5))
                .updateBy(UpdateByOperation.RollingGroup("ts", rev, fwd, "x"), "Sym").ungroup("x");

        final Callable<Table> callable = () -> grouped.ungroup("x");

        updateGraph.startCycleForUnitTests(false);

        final Table ungroup1 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertTableEquals(expect1, ungroup1);

        TstUtils.addToTable(table, i(3, 8),
                col("Sym", "b", "b"),
                instantCol("ts", baseTime.plusSeconds(5), baseTime.plusSeconds(30)),
                intCol("x", 4, 5));
        table.notifyListeners(i(3, 8), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        // the rolling group has not yet processed the update, so instantiation must use previous values
        final Table ungroup2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(expect1, prevTable(ungroup1));
        assertTableEquals(expect1, prevTable(ungroup2));

        updateGraph.completeCycleForUnitTests();

        assertTableEquals(expect2, ungroup1);
        assertTableEquals(expect2, ungroup2);
    }

    @Test
    public void testUpdateView() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"));
        final Table tableStart =
                TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                        col("x", 1, 2, 3), col("y", "a", "b", "c"), col("z", 4, 8, 12));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(2, 3, 4, 6).toTracking(),
                col("x", 1, 4, 2, 3), col("y", "a", "d", "b", "c"), col("z", 4, 16, 8, 12));

        final Callable<Table> callable = () -> table.updateView("z=x*4");

        updateGraph.startCycleForUnitTests();

        final Table updateView1 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(updateView1, tableStart);

        TstUtils.addToTable(table, i(3), col("x", 4), col("y", "d"));

        final Table updateView2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(updateView1));
        TstUtils.assertTableEquals(tableStart, prevTable(updateView2));

        table.notifyListeners(i(3), i(), i());

        final Table updateView3 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(updateView1));
        TstUtils.assertTableEquals(tableStart, prevTable(updateView2));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, updateView1);
        TstUtils.assertTableEquals(tableUpdate, updateView2);
        TstUtils.assertTableEquals(tableUpdate, updateView3);
    }

    @Test
    public void testView() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("y", "a", "b", "c"), col("z", 4, 8, 12));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(2, 3, 4, 6).toTracking(),
                col("y", "a", "d", "b", "c"), col("z", 4, 16, 8, 12));

        final Callable<Table> callable = () -> table.view("y", "z=x*4");

        updateGraph.startCycleForUnitTests();

        final Table updateView1 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(updateView1, tableStart);

        TstUtils.addToTable(table, i(3), col("x", 4), col("y", "d"));

        final Table updateView2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(updateView1));
        TstUtils.assertTableEquals(tableStart, prevTable(updateView2));

        table.notifyListeners(i(3), i(), i());

        final Table updateView3 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(updateView1));
        TstUtils.assertTableEquals(tableStart, prevTable(updateView2));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, updateView1);
        TstUtils.assertTableEquals(tableUpdate, updateView2);
        TstUtils.assertTableEquals(tableUpdate, updateView3);
    }

    @Test
    public void testShiftedColumnsConcurrent()
            throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3));

        final String shiftedColName = new ShiftedColumnDefinition("x", -1).getResultColumnName();
        final Callable<Table> callable = () -> ShiftedColumnOperation.addShiftedColumns(
                table, new ShiftedColumnDefinition("x", -1));

        final Table tableStart = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3),
                col(shiftedColName, NULL_INT, 1, 2));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(2, 3, 4, 6).toTracking(),
                col("x", 1, 4, 2, 3),
                col(shiftedColName, NULL_INT, 1, 4, 2));

        updateGraph.startCycleForUnitTests(false);

        // Mutate the source mid-cycle BEFORE the shifted operation is created.
        // Without an OperationSnapshotControl, ShiftedColumnOperation would
        // register its listener against state that already incorporates this
        // addition (the result table shares the source's live TrackingRowSet)
        // while the source still has the addition queued for notification —
        // the listener then re-applies the update and the result is notified
        // twice in the same step.
        TstUtils.addToTable(table, i(3), col("x", 4));

        final Table shifted = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        // Attach a FailureListener BEFORE completing the cycle so any
        // listener-side error during cycle completion surfaces through it.
        final FailureListener failureListener = new FailureListener();
        shifted.addUpdateListener(failureListener);

        // Pre-notification view of `shifted` should reflect the source's
        // pre-cycle state.
        TstUtils.assertTableEquals(tableStart, prevTable(shifted));

        table.notifyListeners(i(3), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();
        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, shifted);
    }

    @Test
    public void testUpdateViewShifted() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3),
                col("y", NULL_INT, 1, 2));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(2, 3, 4, 6).toTracking(),
                col("x", 1, 4, 2, 3),
                col("y", NULL_INT, 1, 4, 2));

        final Callable<Table> callable = () -> table.updateView("y = x_[i-1]");

        updateGraph.startCycleForUnitTests(false);

        final Table view1 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(view1, tableStart);

        TstUtils.addToTable(table, i(3), col("x", 4));

        final Table view2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(view1));
        TstUtils.assertTableEquals(tableStart, prevTable(view2));

        table.notifyListeners(i(3), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table view3 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(view1));
        TstUtils.assertTableEquals(tableStart, prevTable(view2));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, view1);
        TstUtils.assertTableEquals(tableUpdate, view2);
        TstUtils.assertTableEquals(tableUpdate, view3);
    }

    @Test
    public void testDropColumns() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table =
                TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                        col("x", 1, 2, 3), col("y", "a", "b", "c"), col("z", 4, 8, 12));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(2, 3, 4, 6).toTracking(),
                col("x", 1, 4, 2, 3), col("y", "a", "d", "b", "c"));

        final Callable<Table> callable = () -> table.dropColumns("z");

        updateGraph.startCycleForUnitTests();

        final Table dropColumns1 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(dropColumns1, tableStart);

        TstUtils.addToTable(table, i(3), col("x", 4), col("y", "d"), col("z", 16));

        final Table dropColumns2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(dropColumns1));
        TstUtils.assertTableEquals(tableStart, prevTable(dropColumns2));

        table.notifyListeners(i(3), i(), i());

        final Table dropColumns3 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(dropColumns1));
        TstUtils.assertTableEquals(tableStart, prevTable(dropColumns2));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, dropColumns1);
        TstUtils.assertTableEquals(tableUpdate, dropColumns2);
        TstUtils.assertTableEquals(tableUpdate, dropColumns3);
    }

    @Test
    public void testWhere() throws ExecutionException, InterruptedException, TimeoutException {
        testWhereInternal(false);
    }

    @Test
    public void testWhereIndexed() throws ExecutionException, InterruptedException, TimeoutException {
        testWhereInternal(true);
    }

    private void testWhereInternal(final boolean indexed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"), col("z", true, false, true));
        if (indexed) {
            DataIndexer.getOrCreateDataIndex(table, "z");
        }
        final Table tableStart =
                TstUtils.testRefreshingTable(i(2, 6).toTracking(),
                        col("x", 1, 3), col("y", "a", "c"), col("z", true, true));

        updateGraph.startCycleForUnitTests(false);

        final Table filter1 = pool.submit(() -> table.where("z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(filter1, tableStart);

        TstUtils.addToTable(table, i(3), col("x", 4), col("y", "d"), col("z", true));

        final Table filter2 = pool.submit(() -> table.where("z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        table.notifyListeners(i(3), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        if (indexed) {
            final Table indexTable = DataIndexer.getDataIndex(table, "z").table();
            Assert.eqFalse(indexTable.satisfied(updateGraph.clock().currentStep()), "indexTable.satisfied");

            // The next where() call will depend on the index table. Make sure it is satisfied before proceeding.
            while (!indexTable.satisfied(updateGraph.clock().currentStep())) {
                updateGraph.flushOneNotificationForUnitTests();
            }
            Assert.eqTrue(indexTable.satisfied(updateGraph.clock().currentStep()), "indexTable.satisfied");
        }

        final Table filter3 = pool.submit(() -> table.where("z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        updateGraph.completeCycleForUnitTests();

        final Table tableUpdate = TstUtils.testRefreshingTable(i(2, 3, 6).toTracking(),
                col("x", 1, 4, 3), col("y", "a", "d", "c"), col("z", true, true, true));

        TstUtils.assertTableEquals(tableUpdate, filter1);
        TstUtils.assertTableEquals(tableUpdate, filter2);
        TstUtils.assertTableEquals(tableUpdate, filter3);
    }

    @Test
    public void testWhere2() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"), col("z", true, false, true));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 6).toTracking(),
                col("x", 1, 3), col("y", "a", "c"), col("z", true, true));
        final Table testUpdate = TstUtils.testRefreshingTable(i(3, 6).toTracking(),
                col("x", 4, 3), col("y", "d", "c"), col("z", true, true));

        updateGraph.startCycleForUnitTests(false);

        final Table filter1 = pool.submit(() -> table.where("z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(filter1, tableStart);

        TstUtils.addToTable(table, i(2, 3), col("x", 1, 4), col("y", "a", "d"), col("z", false, true));

        final Table filter2 = pool.submit(() -> table.where("z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        table.notifyListeners(i(3), i(), i(2));
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table filter3 = pool.submit(() -> table.where("z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        updateGraph.completeCycleForUnitTests();

        showWithRowSet(table);
        showWithRowSet(filter1);
        showWithRowSet(filter2);
        showWithRowSet(filter3);

        TstUtils.assertTableEquals(testUpdate, filter1);
        TstUtils.assertTableEquals(testUpdate, filter2);
        TstUtils.assertTableEquals(testUpdate, filter3);
    }

    @Test
    public void testWhereSortedColumnBinarySearchAsc()
            throws ExecutionException, InterruptedException, TimeoutException {
        testWhereSortedColumnBinarySearchInternal(true);
    }

    @Test
    public void testWhereSortedColumnBinarySearchDesc()
            throws ExecutionException, InterruptedException, TimeoutException {
        testWhereSortedColumnBinarySearchInternal(false);
    }

    @Test
    public void testWhereSortedColumnBinarySearchStringAsc()
            throws ExecutionException, InterruptedException, TimeoutException {
        testWhereSortedColumnBinarySearchStringInternal(true);
    }

    @Test
    public void testWhereSortedColumnBinarySearchStringDesc()
            throws ExecutionException, InterruptedException, TimeoutException {
        testWhereSortedColumnBinarySearchStringInternal(false);
    }

    /**
     * Verifies that binary-search pushdown on a sorted refreshing table correctly handles both {@code usePrev=true}
     * (snapshot during an active update cycle) and {@code usePrev=false} (post-cycle current state). Filters
     * constructed while the update cycle is in progress must see the previous sorted-column values; filters observed
     * after the cycle completes must reflect the newly added rows.
     * <p>
     * Both a range filter ({@code Sentinel >= 5 && Sentinel <= 7}) and an equivalent match filter
     * ({@code Sentinel in 5, 6, 7}) are tested, since
     * {@link io.deephaven.engine.table.impl.sort.SortedColumnPushdownManager} dispatches to different binary-search
     * kernels for each.
     * <p>
     * IntColumnBinarySearchKernel is tested as a replication for all primitive types.
     */
    private void testWhereSortedColumnBinarySearchInternal(final boolean ascending)
            throws ExecutionException, InterruptedException, TimeoutException {
        // Source has Sentinel values [1, 3, 5, 7, 9] — sparse row keys to confirm the binary search
        // correctly translates positions to row keys via RowSet.get().
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10).toTracking(),
                intCol("Sentinel", 1, 3, 5, 7, 9));

        // sort()/sortDescending() attaches SortedColumnsAttribute, which causes AbstractFilterExecution
        // to build a SortedColumnPushdownManager for range/match filters on the Sentinel column.
        final Table sorted = ascending ? source.sort("Sentinel") : source.sortDescending("Sentinel");

        // Range filter expressed as string predicates; equivalent to IntRangeFilter("Sentinel", 5, 7, true, true).
        final Filter rangeFilter = Filter.and(Filter.from("Sentinel >= 5", "Sentinel <= 7"));
        // IntRangeFilter directly — exercises the typed range binary-search kernel.
        final Filter intRangeFilter = new IntRangeFilter("Sentinel", 5, 7, true, true);
        // Match filter: Sentinel in {5, 6, 7} — same result set as the range filter after the update.
        final Filter matchFilter = Filter.and(Filter.from("Sentinel in 5, 6, 7"));

        // Initial filter expectation: Sentinel in [5, 7] inclusive → {5, 7} in sort order.
        final Table tableStart = ascending
                ? newTable(intCol("Sentinel", 5, 7))
                : newTable(intCol("Sentinel", 7, 5));

        updateGraph.startCycleForUnitTests(false);

        // Filters constructed before any data change during the active cycle.
        // ConstructSnapshot will use usePrev=true, but prev == current so results are {5, 7}.
        final Table rangeFilter1 = pool.submit(() -> sorted.where(rangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table intRangeFilter1 = pool.submit(() -> sorted.where(intRangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table matchFilter1 = pool.submit(() -> sorted.where(matchFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        TstUtils.assertTableEquals(tableStart, rangeFilter1);
        TstUtils.assertTableEquals(tableStart, intRangeFilter1);
        TstUtils.assertTableEquals(tableStart, matchFilter1);

        // Add two new rows with Sentinel values 4 and 6 — NOT yet propagated to listeners.
        TstUtils.addToTable(source, i(12, 14), intCol("Sentinel", 4, 6));

        // Filters constructed after addToTable but before notifyListeners.
        // ConstructSnapshot still uses usePrev=true because sorted is not yet satisfied for this cycle.
        // The binary search must read prev column values and return the pre-update result {5, 7}.
        final Table rangeFilter2 = pool.submit(() -> sorted.where(rangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table intRangeFilter2 = pool.submit(() -> sorted.where(intRangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table matchFilter2 = pool.submit(() -> sorted.where(matchFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        // All filters' prev-state snapshots must equal the pre-cycle data {5, 7} in sort order.
        TstUtils.assertTableEquals(tableStart, prevTable(rangeFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(intRangeFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(matchFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(rangeFilter2));
        TstUtils.assertTableEquals(tableStart, prevTable(intRangeFilter2));
        TstUtils.assertTableEquals(tableStart, prevTable(matchFilter2));

        source.notifyListeners(i(12, 14), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        // Filters constructed after source is satisfied, but sorted's downstream listeners have not yet
        // fired. ConstructSnapshot uses usePrev=true until sorted itself is satisfied.
        final Table rangeFilter3 = pool.submit(() -> sorted.where(rangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table intRangeFilter3 = pool.submit(() -> sorted.where(intRangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table matchFilter3 = pool.submit(() -> sorted.where(matchFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(rangeFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(intRangeFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(matchFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(rangeFilter2));
        TstUtils.assertTableEquals(tableStart, prevTable(intRangeFilter2));
        TstUtils.assertTableEquals(tableStart, prevTable(matchFilter2));

        updateGraph.completeCycleForUnitTests();

        // After the cycle, sorted contains [1, 3, 4, 5, 6, 7, 9] (asc) or [9, 7, 6, 5, 4, 3, 1] (desc).
        // All three filters match {5, 6, 7} in sort order.
        // All refreshing filter tables must have updated to reflect usePrev=false current values.
        final Table tableUpdate = ascending
                ? newTable(intCol("Sentinel", 5, 6, 7))
                : newTable(intCol("Sentinel", 7, 6, 5));
        TstUtils.assertTableEquals(tableUpdate, rangeFilter1);
        TstUtils.assertTableEquals(tableUpdate, intRangeFilter1);
        TstUtils.assertTableEquals(tableUpdate, matchFilter1);
        TstUtils.assertTableEquals(tableUpdate, rangeFilter2);
        TstUtils.assertTableEquals(tableUpdate, intRangeFilter2);
        TstUtils.assertTableEquals(tableUpdate, matchFilter2);
        TstUtils.assertTableEquals(tableUpdate, rangeFilter3);
        TstUtils.assertTableEquals(tableUpdate, intRangeFilter3);
        TstUtils.assertTableEquals(tableUpdate, matchFilter3);
    }

    /**
     * String-typed clone of {@link #testWhereSortedColumnBinarySearchInternal}. Uses {@link ComparableRangeFilter} in
     * place of {@link IntRangeFilter} to exercise the Object binary-search kernel path through
     * {@link io.deephaven.engine.table.impl.sort.SortedColumnPushdownManager}.
     */
    private void testWhereSortedColumnBinarySearchStringInternal(final boolean ascending)
            throws ExecutionException, InterruptedException, TimeoutException {
        // Source has Sentinel values ["a","c","e","g","i"] — sparse row keys to confirm the binary search
        // correctly translates positions to row keys via RowSet.get().
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10).toTracking(),
                stringCol("Sentinel", "a", "c", "e", "g", "i"));

        // sort()/sortDescending() attaches SortedColumnsAttribute, which causes AbstractFilterExecution
        // to build a SortedColumnPushdownManager for range/match filters on the Sentinel column.
        final Table sorted = ascending ? source.sort("Sentinel") : source.sortDescending("Sentinel");

        // Range filter expressed as string predicates.
        final Filter rangeFilter = Filter.and(Filter.from("Sentinel >= `e`", "Sentinel <= `g`"));
        // ComparableRangeFilter directly — exercises the Object range binary-search kernel.
        final Filter comparableRangeFilter =
                ComparableRangeFilter.makeForTest("Sentinel", "e", "g", true, true);
        // Match filter: Sentinel in {"e","f","g"} — same result set as the range filter after the update.
        final Filter matchFilter = Filter.and(Filter.from("Sentinel in `e`, `f`, `g`"));

        // Initial filter expectation: Sentinel in ["e","g"] inclusive → {"e","g"} in sort order.
        final Table tableStart = ascending
                ? newTable(stringCol("Sentinel", "e", "g"))
                : newTable(stringCol("Sentinel", "g", "e"));

        updateGraph.startCycleForUnitTests(false);

        // Filters constructed before any data change during the active cycle.
        // ConstructSnapshot will use usePrev=true, but prev == current so results are {"e","g"}.
        final Table rangeFilter1 = pool.submit(() -> sorted.where(rangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table comparableRangeFilter1 =
                pool.submit(() -> sorted.where(comparableRangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table matchFilter1 = pool.submit(() -> sorted.where(matchFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        TstUtils.assertTableEquals(tableStart, rangeFilter1);
        TstUtils.assertTableEquals(tableStart, comparableRangeFilter1);
        TstUtils.assertTableEquals(tableStart, matchFilter1);

        // Add two new rows with Sentinel values "d" and "f" — NOT yet propagated to listeners.
        TstUtils.addToTable(source, i(12, 14), stringCol("Sentinel", "d", "f"));

        // Filters constructed after addToTable but before notifyListeners.
        // ConstructSnapshot still uses usePrev=true because sorted is not yet satisfied for this cycle.
        // The binary search must read prev column values and return the pre-update result {"e","g"}.
        final Table rangeFilter2 = pool.submit(() -> sorted.where(rangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table comparableRangeFilter2 =
                pool.submit(() -> sorted.where(comparableRangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table matchFilter2 = pool.submit(() -> sorted.where(matchFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        // All filters' prev-state snapshots must equal the pre-cycle data {"e","g"} in sort order.
        TstUtils.assertTableEquals(tableStart, prevTable(rangeFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(comparableRangeFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(matchFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(rangeFilter2));
        TstUtils.assertTableEquals(tableStart, prevTable(comparableRangeFilter2));
        TstUtils.assertTableEquals(tableStart, prevTable(matchFilter2));

        source.notifyListeners(i(12, 14), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        // Filters constructed after source is satisfied, but sorted's downstream listeners have not yet
        // fired. ConstructSnapshot uses usePrev=true until sorted itself is satisfied.
        final Table rangeFilter3 = pool.submit(() -> sorted.where(rangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table comparableRangeFilter3 =
                pool.submit(() -> sorted.where(comparableRangeFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table matchFilter3 = pool.submit(() -> sorted.where(matchFilter)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(rangeFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(comparableRangeFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(matchFilter1));
        TstUtils.assertTableEquals(tableStart, prevTable(rangeFilter2));
        TstUtils.assertTableEquals(tableStart, prevTable(comparableRangeFilter2));
        TstUtils.assertTableEquals(tableStart, prevTable(matchFilter2));

        updateGraph.completeCycleForUnitTests();

        // After the cycle, sorted contains ["a","c","d","e","f","g","i"] (asc) or ["i","g","f","e","d","c","a"] (desc).
        // All three filters match {"e","f","g"} in sort order.
        // All refreshing filter tables must have updated to reflect usePrev=false current values.
        final Table tableUpdate = ascending
                ? newTable(stringCol("Sentinel", "e", "f", "g"))
                : newTable(stringCol("Sentinel", "g", "f", "e"));
        TstUtils.assertTableEquals(tableUpdate, rangeFilter1);
        TstUtils.assertTableEquals(tableUpdate, comparableRangeFilter1);
        TstUtils.assertTableEquals(tableUpdate, matchFilter1);
        TstUtils.assertTableEquals(tableUpdate, rangeFilter2);
        TstUtils.assertTableEquals(tableUpdate, comparableRangeFilter2);
        TstUtils.assertTableEquals(tableUpdate, matchFilter2);
        TstUtils.assertTableEquals(tableUpdate, rangeFilter3);
        TstUtils.assertTableEquals(tableUpdate, comparableRangeFilter3);
        TstUtils.assertTableEquals(tableUpdate, matchFilter3);
    }

    @Test
    public void testIncrementalReleaseFilter() throws ExecutionException, InterruptedException, TimeoutException {
        testIncrementalReleaseFilter(false);
        testIncrementalReleaseFilter(true);
    }

    private void testIncrementalReleaseFilter(final boolean addOnly)
            throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), intCol("x", 1, 2, 3));
        final Table toFilter;
        if (addOnly) {
            toFilter = table.assertAddOnly();
        } else {
            toFilter = table.assertAppendOnly();
        }

        final Table tableStart = table.slice(0, 1).snapshot();

        final Supplier<IncrementalReleaseFilter> releaseFilterSupplier = () -> new IncrementalReleaseFilter(1, 1);

        updateGraph.startCycleForUnitTests(false);

        final Table filter1 =
                pool.submit(() -> toFilter.where(releaseFilterSupplier.get())).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(filter1, table.slice(0, 1));

        final WritableRowSet addRows = addOnly ? i(1, 9) : i(7, 8);
        TstUtils.addToTable(table, addRows, col("x", 0, 5));

        final Table filter2 =
                pool.submit(() -> toFilter.where(releaseFilterSupplier.get())).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        table.notifyListeners(addRows, i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table filter3 =
                pool.submit(() -> table.where(releaseFilterSupplier.get())).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableStart, filter1);
        TstUtils.assertTableEquals(tableStart, filter2);
        TstUtils.assertTableEquals(table.slice(0, 1), filter3);
    }

    @Test
    public void testSort() throws ExecutionException, InterruptedException, TimeoutException {
        testSortInternal(false);
    }

    @Test
    public void testSortIndexed() throws ExecutionException, InterruptedException, TimeoutException {
        testSortInternal(true);
    }

    private void testSortInternal(final boolean indexed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"));
        if (indexed) {
            DataIndexer.getOrCreateDataIndex(table, "x");
        }

        final Table tableStart = TstUtils.testRefreshingTable(i(1, 2, 3).toTracking(),
                col("x", 3, 2, 1), col("y", "c", "b", "a"));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(1, 2, 3, 4).toTracking(),
                col("x", 4, 3, 2, 1), col("y", "d", "c", "b", "a"));

        updateGraph.startCycleForUnitTests(false);

        final Table sort1 = pool.submit(() -> table.sortDescending("x")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(sort1, tableStart);

        TstUtils.addToTable(table, i(3), col("x", 4), col("y", "d"));

        final Table sort2 = pool.submit(() -> table.sortDescending("x")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(sort1));
        TstUtils.assertTableEquals(tableStart, prevTable(sort2));

        table.notifyListeners(i(3), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        updateGraph.flushAllNormalNotificationsForUnitTests();

        final Table sort3 = pool.submit(() -> table.sortDescending("x")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(sort1));
        TstUtils.assertTableEquals(tableStart, prevTable(sort2));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, sort1);
        TstUtils.assertTableEquals(tableUpdate, sort2);
        TstUtils.assertTableEquals(tableUpdate, sort3);
    }

    @Test
    public void testReverse() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"));
        final Table tableStart = TstUtils.testRefreshingTable(i(1, 2, 3).toTracking(),
                col("x", 3, 2, 1), col("y", "c", "b", "a"));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(1, 2, 3, 4).toTracking(),
                col("x", 4, 3, 2, 1), col("y", "d", "c", "b", "a"));
        final Table tableUpdate2 = TstUtils.testRefreshingTable(i(1, 2, 3, 4, 5).toTracking(),
                col("x", 5, 4, 3, 2, 1), col("y", "e", "d", "c", "b", "a"));
        final Table tableUpdate3 = TstUtils.testRefreshingTable(i(1, 2, 3, 4, 5, 6).toTracking(),
                col("x", 6, 5, 4, 3, 2, 1), col("y", "f", "e", "d", "c", "b", "a"));

        updateGraph.startCycleForUnitTests(false);

        final Table reverse1 = pool.submit(table::reverse).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(reverse1, tableStart);

        TstUtils.addToTable(table, i(8), col("x", 4), col("y", "d"));

        final Table reverse2 = pool.submit(table::reverse).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(reverse1));
        TstUtils.assertTableEquals(tableStart, prevTable(reverse2));

        table.notifyListeners(i(8), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table reverse3 = pool.submit(table::reverse).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(reverse1));
        TstUtils.assertTableEquals(tableStart, prevTable(reverse2));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, reverse1);
        TstUtils.assertTableEquals(tableUpdate, reverse2);
        TstUtils.assertTableEquals(tableUpdate, reverse3);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(10000), col("x", 5), col("y", "e"));
            table.notifyListeners(i(10000), i(), i());
        });
        TableTools.show(reverse1);
        TableTools.show(reverse2);
        TableTools.show(reverse3);
        assertTableEquals(tableUpdate2, reverse1);
        assertTableEquals(tableUpdate2, reverse2);
        assertTableEquals(tableUpdate2, reverse3);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(10001), col("x", 6), col("y", "f"));
            table.notifyListeners(i(10001), i(), i());
        });
        TableTools.show(reverse1);
        TableTools.show(reverse2);
        TableTools.show(reverse3);
        assertTableEquals(tableUpdate3, reverse1);
        assertTableEquals(tableUpdate3, reverse2);
        assertTableEquals(tableUpdate3, reverse3);
    }

    @Test
    public void testUngroup() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                intCol("Key", 1, 2, 3), col("Value", new int[] {101}, new int[] {201, 202}, new int[] {301}));

        final Table withVector = table.update("Value=new io.deephaven.vector.IntVectorDirect(Value)");

        final Table tableStart = TableTools.newTable(intCol("Key", 1, 2, 2, 3), intCol("Value", 101, 201, 202, 301));
        final Table tableUpdate =
                TableTools.newTable(intCol("Key", 1, 2, 2, 3, 4), intCol("Value", 101, 201, 202, 301, 401));
        final Table tableUpdate2 = TableTools.newTable(intCol("Key", 1, 2, 2, 3, 4, 5, 5),
                intCol("Value", 101, 201, 202, 301, 401, 501, 502));
        final Table tableUpdate3 = TableTools.newTable(intCol("Key", 1, 2, 2, 3, 4, 6, 6, 6),
                intCol("Value", 101, 201, 202, 301, 401, 601, 602, 603));

        updateGraph.startCycleForUnitTests(false);

        final Table ungroup1 = pool.submit(() -> table.ungroup()).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table ungroupv1 = pool.submit(() -> withVector.ungroup()).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(ungroup1, tableStart);
        assertTableEquals(ungroupv1, tableStart);

        TstUtils.addToTable(table, i(8), intCol("Key", 4), col("Value", new int[] {401}));

        final Table ungroup2 = pool.submit(() -> table.ungroup()).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table ungroupv2 = pool.submit(() -> withVector.ungroup()).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(ungroup1));
        TstUtils.assertTableEquals(tableStart, prevTable(ungroup2));
        TstUtils.assertTableEquals(tableStart, prevTable(ungroupv1));
        TstUtils.assertTableEquals(tableStart, prevTable(ungroup2));

        table.notifyListeners(i(8), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table ungroup3 = pool.submit(() -> table.ungroup()).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table ungroupv3 = pool.submit(() -> withVector.ungroup()).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(ungroup1));
        TstUtils.assertTableEquals(tableStart, prevTable(ungroup2));
        TstUtils.assertTableEquals(tableUpdate, ungroup3);
        TstUtils.assertTableEquals(tableStart, prevTable(ungroupv1));
        TstUtils.assertTableEquals(tableStart, prevTable(ungroupv2));
        // ungroup v3 doesn't actually have the data yet
        TstUtils.assertTableEquals(tableStart, prevTable(ungroupv3));

        while (((BaseTable) withVector).getLastNotificationStep() < updateGraph.clock().currentStep()) {
            updateGraph.flushOneNotificationForUnitTests();
        }

        final Table ungroupv4 = pool.submit(() -> withVector.ungroup()).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        TstUtils.assertTableEquals(tableUpdate, ungroupv4);

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, ungroup1);
        TstUtils.assertTableEquals(tableUpdate, ungroup2);
        TstUtils.assertTableEquals(tableUpdate, ungroup3);
        TstUtils.assertTableEquals(tableUpdate, ungroupv1);
        TstUtils.assertTableEquals(tableUpdate, ungroupv2);
        TstUtils.assertTableEquals(tableUpdate, ungroupv3);
        TstUtils.assertTableEquals(tableUpdate, ungroupv4);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(10000), intCol("Key", 5), col("Value", new int[] {501, 502}));
            table.notifyListeners(i(10000), i(), i());
        });
        assertTableEquals(tableUpdate2, ungroup1);
        assertTableEquals(tableUpdate2, ungroup2);
        assertTableEquals(tableUpdate2, ungroup3);
        assertTableEquals(tableUpdate2, ungroupv1);
        assertTableEquals(tableUpdate2, ungroupv2);
        assertTableEquals(tableUpdate2, ungroupv3);
        assertTableEquals(tableUpdate2, ungroupv4);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(10000), col("Key", 6), col("Value", new int[] {601, 602, 603}));
            table.notifyListeners(i(), i(), i(10000));
        });
        assertTableEquals(tableUpdate3, ungroup1);
        assertTableEquals(tableUpdate3, ungroup2);
        assertTableEquals(tableUpdate3, ungroup3);
        assertTableEquals(tableUpdate3, ungroupv1);
        assertTableEquals(tableUpdate3, ungroupv2);
        assertTableEquals(tableUpdate3, ungroupv3);
        assertTableEquals(tableUpdate3, ungroupv4);
    }

    @Test
    public void testUngroupBadSize() throws ExecutionException, InterruptedException, TimeoutException {
        testUngroupBadSize(t -> t);
        testUngroupBadSize(t -> QueryTableUngroupTest.convertToUngroupable(t, "Value", "Value2"));
    }

    private void testUngroupBadSize(final Function<Table, Table> transform)
            throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4).toTracking(),
                intCol("Key", 1, 2),
                col("Value", new int[] {101}, new int[] {201}),
                col("Value2", new int[] {1001}, new int[] {2001, 2002}));
        final Table transformed = transform.apply(table);

        updateGraph.startCycleForUnitTests(false);

        final Future<Table> submit1 = pool.submit(() -> transformed.ungroup());
        // we need to complete the cycle, because when a snapshot fails with previous values we attempt with a lock
        Thread.sleep(TIMEOUT_LENGTH / 2);

        TstUtils.addToTable(table, i(8), intCol("Key", 4), col("Value", new int[] {401}),
                col("Value2", new int[] {4001}));

        final Future<Table> submit2 = pool.submit(() -> transformed.ungroup());

        table.notifyListeners(i(8), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Future<Table> submit3 = pool.submit(() -> transformed.ungroup());
        Thread.sleep(TIMEOUT_LENGTH / 2);

        while (((BaseTable) transformed).getLastNotificationStep() < updateGraph.clock().currentStep()) {
            updateGraph.flushOneNotificationForUnitTests();
        }
        final Future<Table> submit4 = pool.submit(() -> transformed.ungroup());
        Thread.sleep(TIMEOUT_LENGTH / 2);

        updateGraph.completeCycleForUnitTests();

        checkUngroupSizeError(submit1);
        checkUngroupSizeError(submit2);
        checkUngroupSizeError(submit3);
        checkUngroupSizeError(submit4);
    }

    private static void checkUngroupSizeError(final Future<Table> future) {
        final ExecutionException ee1 = org.junit.Assert.assertThrows(ExecutionException.class,
                () -> future.get(TIMEOUT_LENGTH, TIMEOUT_UNIT));
        final IllegalStateException ise1 = (IllegalStateException) ee1.getCause();
        assertEquals("Array sizes differ at row key 4 (position 1), Value has size 1, Value2 has size 2",
                ise1.getMessage());
    }

    @Test
    public void testUngroupSizeChanges() throws ExecutionException, InterruptedException, TimeoutException {
        testUngroupTransformed(false, t -> t.update("Value=new io.deephaven.vector.IntVectorDirect(Value)"));
        testUngroupTransformed(false, t -> t.update("Value2=new io.deephaven.vector.DoubleVectorDirect(Value2)"));
        testUngroupTransformed(true, t -> t.update("Value=new io.deephaven.vector.IntVectorDirect(Value)"));
        testUngroupTransformed(true, t -> t.update("Value2=new io.deephaven.vector.DoubleVectorDirect(Value2)"));
    }

    @Test
    public void testUngroupUngroupableColumnSource() throws ExecutionException, InterruptedException, TimeoutException {
        testUngroupUngroupableColumnSource(false);
        testUngroupUngroupableColumnSource(true);
    }

    private void testUngroupUngroupableColumnSource(final boolean nullFill)
            throws ExecutionException, InterruptedException, TimeoutException {
        testUngroupTransformed(nullFill, t -> QueryTableUngroupTest.convertToUngroupable(t, "Value"));
        testUngroupTransformed(nullFill, t -> QueryTableUngroupTest.convertToUngroupable(t, "Value2"));
    }

    private void testUngroupTransformed(final boolean nullFill, final Function<Table, Table> transformation)
            throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                intCol("Key", 1, 2, 3),
                col("Value", new int[] {101}, new int[] {201, 202}, new int[] {301}),
                col("Value2", new double[] {1.01}, new double[] {2.01, 2.02}, new double[] {3.01}));

        final Table withVector = transformation.apply(table);

        final Table tableStart = TableTools.newTable(intCol("Key", 1, 2, 2, 3), intCol("Value", 101, 201, 202, 301))
                .update("Value2=Value/100");
        final Table tableUpdate =
                TableTools.newTable(intCol("Key", 4, 4, 4, 5, 3), intCol("Value", 401, 402, 403, 501, 301))
                        .update("Value2=Value/100");
        final Table tableUpdate2 = TableTools.newTable(intCol("Key", 4, 4, 4, 5, 3, 6, 6),
                intCol("Value", 401, 402, 403, 501, 301, 601, 602)).update("Value2=Value/100");
        final Table tableUpdate3 =
                TableTools.newTable(intCol("Key", 4, 4, 4, 5, 3, 7), intCol("Value", 401, 402, 403, 501, 301, 701))
                        .update("Value2=Value/100");

        updateGraph.startCycleForUnitTests(false);

        final Table ungroup1 = pool.submit(() -> table.ungroup(nullFill)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table ungroupv1 = pool.submit(() -> withVector.ungroup(nullFill)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(ungroup1, tableStart);
        assertTableEquals(ungroupv1, tableStart);

        TableTools.showWithRowSet(ungroup1);

        TstUtils.addToTable(table, i(2, 4), intCol("Key", 4, 5),
                col("Value", new int[] {401, 402, 403}, new int[] {501}),
                col("Value2", new double[] {4.01, 4.02, 4.03}, new double[] {5.01}));

        final Table ungroup2 = pool.submit(() -> table.ungroup(nullFill)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table ungroupv2 = pool.submit(() -> withVector.ungroup(nullFill)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TableTools.showWithRowSet(ungroup2);
        TableTools.showWithRowSet(ungroupv2);

        TstUtils.assertTableEquals(tableStart, prevTable(ungroup1));
        TstUtils.assertTableEquals(tableStart, prevTable(ungroup2));
        TstUtils.assertTableEquals(tableStart, prevTable(ungroupv1));
        TstUtils.assertTableEquals(tableStart, prevTable(ungroupv2));

        table.notifyListeners(i(), i(), i(2, 4));
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table ungroup3 = pool.submit(() -> table.ungroup(nullFill)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table ungroupv3 = pool.submit(() -> withVector.ungroup(nullFill)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(ungroup1));
        TstUtils.assertTableEquals(tableStart, prevTable(ungroup2));
        TstUtils.assertTableEquals(tableUpdate, ungroup3);
        TstUtils.assertTableEquals(tableStart, prevTable(ungroupv1));
        TstUtils.assertTableEquals(tableStart, prevTable(ungroupv2));
        // ungroup v3 doesn't actually have the data yet
        TstUtils.assertTableEquals(tableStart, prevTable(ungroupv3));

        while (((BaseTable) withVector).getLastNotificationStep() < updateGraph.clock().currentStep()) {
            updateGraph.flushOneNotificationForUnitTests();
        }

        final Table ungroupv4 = pool.submit(() -> withVector.ungroup(nullFill)).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        TstUtils.assertTableEquals(tableUpdate, ungroupv4);

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, ungroup1);
        TstUtils.assertTableEquals(tableUpdate, ungroup2);
        TstUtils.assertTableEquals(tableUpdate, ungroup3);
        TstUtils.assertTableEquals(tableUpdate, ungroupv1);
        TstUtils.assertTableEquals(tableUpdate, ungroupv2);
        TstUtils.assertTableEquals(tableUpdate, ungroupv3);
        TstUtils.assertTableEquals(tableUpdate, ungroupv4);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(10000), intCol("Key", 6), col("Value", new int[] {601, 602}),
                    col("Value2", new double[] {6.01, 6.02}));
            table.notifyListeners(i(10000), i(), i());
        });
        assertTableEquals(tableUpdate2, ungroup1);
        assertTableEquals(tableUpdate2, ungroup2);
        assertTableEquals(tableUpdate2, ungroup3);
        assertTableEquals(tableUpdate2, ungroupv1);
        assertTableEquals(tableUpdate2, ungroupv2);
        assertTableEquals(tableUpdate2, ungroupv3);
        assertTableEquals(tableUpdate2, ungroupv4);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(10000), col("Key", 7), col("Value", new int[] {701}),
                    col("Value2", new double[] {7.01}));
            table.notifyListeners(i(), i(), i(10000));
        });
        assertTableEquals(tableUpdate3, ungroup1);
        assertTableEquals(tableUpdate3, ungroup2);
        assertTableEquals(tableUpdate3, ungroup3);
        assertTableEquals(tableUpdate3, ungroupv1);
        assertTableEquals(tableUpdate3, ungroupv2);
        assertTableEquals(tableUpdate3, ungroupv3);
        assertTableEquals(tableUpdate3, ungroupv4);
    }

    @Test
    public void testSortOfPartitionBy() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "a", "a"));
        final PartitionedTable pt = table.partitionBy("y");

        updateGraph.startCycleForUnitTests();

        TstUtils.addToTable(table, i(3), col("x", 4), col("y", "d"));

        table.notifyListeners(i(3), i(), i());

        // We need to flush two notifications: one for the source table and one for the "withView" table in the
        // aggregation helper.
        updateGraph.flushOneNotificationForUnitTests();
        updateGraph.flushOneNotificationForUnitTests();

        final Table tableA = pt.constituentFor("a");
        final Table tableD = pt.constituentFor("d");

        TableTools.show(tableA);
        TableTools.show(tableD);

        final Table sortA = pool.submit(() -> tableA.sort("x")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        final Table sortD = pool.submit(() -> tableD.sort("x")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TableTools.show(sortA);
        TableTools.show(sortD);

        TstUtils.assertTableEquals(tableD, sortD);

        updateGraph.completeCycleForUnitTests();
    }

    @Test
    public void testConstructSnapshotException() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6, 8).toTracking(),
                col("y", "a", "b", "c", "d"));

        final Future<String[]> future = pool.submit(() -> {
            final MutableObject<String[]> result = new MutableObject<>();
            ConstructSnapshot.callDataSnapshotFunction("testConstructSnapshotException",
                    ConstructSnapshot.makeSnapshotControl(false, table.isRefreshing(), table), (usePrev, clock) -> {
                        Assert.eqFalse(usePrev, "usePrev");
                        final int size = table.intSize();
                        final String[] result1 = new String[size];
                        result.setValue(result1);
                        // on the first pass, we want to have an AAIOBE for the result1, which will occur, because 100ms
                        // into this sleep; the RowSet size will increase by 1
                        SleepUtil.sleep(1000);

                        // and make sure the terrible thing has happened
                        if (result1.length == 4) {
                            Assert.eq(table.getRowSet().size(), "table.build().size()", 5);
                        }

                        final ColumnSource<String> cs = table.getColumnSource("y");

                        int ii = 0;
                        for (final RowSet.Iterator it = table.getRowSet().iterator(); it.hasNext();) {
                            final long key = it.nextLong();
                            result1[ii++] = cs.get(key);
                        }

                        return true;
                    });
            return result.getValue();
        });

        // wait until we've had the future start, but before it's actually gotten completed, so we know that it is
        // going to be kicked off in the idle cycle
        SleepUtil.sleep(100);

        // add a row to the table
        updateGraph.startCycleForUnitTests();
        TstUtils.addToTable(table, i(10), col("y", "e"));
        table.notifyListeners(i(10), i(), i());
        updateGraph.completeCycleForUnitTests();

        // now get the answer
        final String[] answer = future.get(5000, TimeUnit.MILLISECONDS);

        assertEquals(Arrays.asList("a", "b", "c", "d", "e"), Arrays.asList(answer));
    }

    @Test
    public void testStaticSnapshot() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"), col("z", true, false, true));
        final Table tableStart =
                TableTools.newTable(col("x", 1, 2, 3), col("y", "a", "b", "c"), col("z", true, false, true));
        final Table tableUpdate =
                TableTools.newTable(col("x", 1, 4, 2, 3), col("y", "a", "d", "b", "c"),
                        col("z", true, true, false, true));

        updateGraph.startCycleForUnitTests(false);

        final Table snap1 = pool.submit(() -> table.snapshot()).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(snap1, tableStart);

        TstUtils.addToTable(table, i(3), col("x", 4), col("y", "d"), col("z", true));

        final Table snap2 = pool.submit(() -> table.snapshot()).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(snap1));
        TstUtils.assertTableEquals(tableStart, prevTable(snap2));

        table.notifyListeners(i(3), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table snap3 = pool.submit(() -> table.snapshot()).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(snap1));
        TstUtils.assertTableEquals(tableStart, prevTable(snap2));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableStart, snap1);
        TstUtils.assertTableEquals(tableStart, snap2);
        TstUtils.assertTableEquals(tableUpdate, snap3);
    }

    @Test
    public void testSnapshotLiveness() {
        final QueryTable trigger, base, snap;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            base = TstUtils.testRefreshingTable(i(0).toTracking(), col("x", 1));
            trigger = TstUtils.testRefreshingTable(i().toTracking());
            snap = (QueryTable) base.snapshotWhen(trigger, Flag.INITIAL);
            snap.retainReference();
        }

        // assert each table is still alive w.r.t. Liveness
        for (final QueryTable t : new QueryTable[] {trigger, base, snap}) {
            t.retainReference();
            t.dropReference();
        }

        TstUtils.assertTableEquals(snap, base);

        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdate downstream1 = new TableUpdateImpl(i(1), i(), i(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
            TstUtils.addToTable(base, downstream1.added(), col("x", 2));
            base.notifyListeners(downstream1);
        });
        TstUtils.assertTableEquals(snap, prevTable(base));

        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdate downstream = new TableUpdateImpl(i(1), i(), i(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
            TstUtils.addToTable(trigger, downstream.added());
            trigger.notifyListeners(downstream);
        });
        TstUtils.assertTableEquals(snap, base);
    }

    @Test
    public void testSourceDependencyWithoutListener() {
        final QueryTable rootTable = TstUtils.testRefreshingTable(i(10).toTracking(), intCol("Sentinel", 10));
        final QueryTable tickTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol("Ticking", 1));

        final ExecutionContext executionContext = ExecutionContext.getContext();

        final InstrumentedTableUpdateListenerAdapter adapter =
                new InstrumentedTableUpdateListenerAdapter(tickTable, true) {
                    @Override
                    public void onUpdate(@NotNull final TableUpdate upstream) {
                        final Table x;
                        try (final SafeCloseable ignored = executionContext.open()) {
                            x = rootTable.updateView("X=Sentinel * 2");
                        }
                        TableTools.showWithRowSet(x);
                    }

                    @Override
                    public boolean canExecute(long step) {
                        return rootTable.satisfied(step) && super.canExecute(step);
                    }
                };
        tickTable.addUpdateListener(adapter);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(tickTable, i(1), intCol("Ticking", 2));
            tickTable.notifyListeners(i(1), i(), i());
        });
    }

    @Test
    public void testMergedTableFilterPushdown() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable source1 = TstUtils.testRefreshingTable(
                RowSetFactory.flat(10).toTracking(),
                col("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        SingleValueColumnSource<?> srcSentinel = SingleValueColumnSource.getSingleValueColumnSource(int.class);
        final Map<String, ColumnSource<?>> columnSourceMap = Map.of("Sentinel", srcSentinel);
        // set the initial value
        srcSentinel.set(11);
        // start tracking prev values
        srcSentinel.startTrackingPrevValues();

        final QueryTable source2 = new QueryTable(RowSetFactory.flat(10).toTracking(), columnSourceMap);
        source2.setRefreshing(true);

        final Table merged = TableTools.merge(source1, source2);

        final Callable<Table> callable = () -> merged.where("Sentinel <= 11");

        updateGraph.startCycleForUnitTests(false);

        final Table filtered1 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        // Test before change.
        assertEquals(20, filtered1.size());
        assertArrayEquals(new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11},
                ColumnVectors.ofInt(filtered1, "Sentinel").toArray());

        TstUtils.addToTable(source1,
                i(10),
                col("Sentinel", 11));
        srcSentinel.set(12);

        // Test after the change, but before the notification (still in prev state)
        final Table filtered2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertEquals(20, filtered2.size());

        source1.notifyListeners(i(10), i(), i());
        source2.notifyListeners(i(), i(), ir(0, 9));

        updateGraph.markSourcesRefreshedForUnitTests();
        updateGraph.completeCycleForUnitTests();

        // Test after the change and notification.
        final Table filtered3 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        assertEquals(11, filtered3.size());
        assertArrayEquals(new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
                ColumnVectors.ofInt(filtered3, "Sentinel").toArray());
    }
}
