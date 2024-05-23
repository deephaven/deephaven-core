//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.base.Pair;
import io.deephaven.base.SleepUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.hierarchical.TreeTableFilter;
import io.deephaven.engine.table.impl.hierarchical.TreeTableImpl;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.BooleanGenerator;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.SortedBy;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.gui.table.QuickFilterMode;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReflexiveUse;
import io.deephaven.util.mutable.MutableInt;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestConcurrentInstantiation extends QueryTableTestBase {
    private static final int TIMEOUT_LENGTH = 10;
    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.SECONDS;

    private ExecutorService pool;
    private ExecutorService dualPool;
    private ControlledUpdateGraph updateGraph;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final ExecutionContext executionContext = ExecutionContext.makeExecutionContext(true);
        final ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    runnable.run();
                }
            });
            thread.setDaemon(true);
            return thread;
        };
        pool = Executors.newFixedThreadPool(1, threadFactory);
        dualPool = Executors.newFixedThreadPool(2, threadFactory);
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        pool.shutdown();
        dualPool.shutdown();
    }

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

        assertArrayEquals(new int[] {1, 3, 4, 6, 9},
                (int[]) DataAccessHelpers.getColumn(rawSorted, "Sentinel").getDirect());

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
                (int[]) DataAccessHelpers.getColumn(rawSorted, "Sentinel").getDirect());
        assertTableEquals(rawSorted, table2);
        assertTableEquals(table2, table3);
        assertTableEquals(table3, table4);
    }

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

    public void testWhere() throws ExecutionException, InterruptedException, TimeoutException {
        testWhereInternal(false);
    }

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
        final Table tableUpdate = TstUtils.testRefreshingTable(i(2, 3, 6).toTracking(),
                col("x", 1, 4, 3), col("y", "a", "d", "c"), col("z", true, true, true));

        updateGraph.startCycleForUnitTests(false);

        final Table filter1 = pool.submit(() -> table.where("z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(filter1, tableStart);

        TstUtils.addToTable(table, i(3), col("x", 4), col("y", "d"), col("z", true));

        final Table filter2 = pool.submit(() -> table.where("z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        table.notifyListeners(i(3), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table filter3 = pool.submit(() -> table.where("z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, filter1);
        TstUtils.assertTableEquals(tableUpdate, filter2);
        TstUtils.assertTableEquals(tableUpdate, filter3);
    }

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

    public void testWhereDynamic() throws ExecutionException, InterruptedException, TimeoutException {
        testWhereDynamicInternal(false, false);
    }

    public void testWhereDynamicIndexedSource() throws ExecutionException, InterruptedException, TimeoutException {
        testWhereDynamicInternal(true, false);
    }

    public void testWhereDynamicIndexedSet() throws ExecutionException, InterruptedException, TimeoutException {
        testWhereDynamicInternal(false, true);
    }

    public void testWhereDynamicIndexedBoth() throws ExecutionException, InterruptedException, TimeoutException {
        testWhereDynamicInternal(true, true);
    }

    private void testWhereDynamicInternal(final boolean sourceIndexed, final boolean setIndexed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"), col("z", true, false, true));
        if (sourceIndexed) {
            DataIndexer.getOrCreateDataIndex(table, "z");
        }
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 6).toTracking(),
                col("x", 1, 3), col("y", "a", "c"), col("z", true, true));
        final Table testUpdate = TstUtils.testRefreshingTable(i(3, 6).toTracking(),
                col("x", 4, 3), col("y", "d", "c"), col("z", true, true));
        final QueryTable whereTable = TstUtils.testRefreshingTable(i(0).toTracking(), col("z", true));
        if (setIndexed) {
            DataIndexer.getOrCreateDataIndex(whereTable, "z");
        }

        // This is something of a silly test, so we've "hacked" the DynamicWhereFilter instance to let us initialize
        // its DataIndex ahead of the operation so that that the where can proceed without a lock.
        // Normally, DynamicWhereFilter is only used from whereIn and whereNotIn, which are not concurrent operations.
        final DynamicWhereFilter filter = updateGraph.sharedLock().computeLocked(
                () -> {
                    final DynamicWhereFilter result =
                            new DynamicWhereFilter(whereTable, true, MatchPairFactory.getExpressions("z")) {
                                private boolean begun;

                                @Override
                                public SafeCloseable beginOperation(@NotNull Table sourceTable) {
                                    if (!begun) {
                                        begun = true;
                                        return super.beginOperation(sourceTable);
                                    }
                                    return () -> {
                                    };
                                }
                            };
                    // noinspection resource
                    result.beginOperation(table);
                    return result;
                });

        updateGraph.startCycleForUnitTests(false);

        final Future<Table> future1 = dualPool.submit(() -> table.where(filter));
        try {
            future1.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
            fail("Filtering should be blocked on UGP because DynamicWhereFilter does not support previous filtering,"
                    + " and so the first where will eventually try to do a locked snapshot");
        } catch (TimeoutException ignored) {
        }
        TstUtils.addToTable(table, i(2, 3), col("x", 1, 4), col("y", "a", "d"), col("z", false, true));

        final Table filter2 = dualPool.submit(() -> table.where("z")).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(tableStart, prevTable(filter2));
        table.notifyListeners(i(3), i(), i(2));
        updateGraph.markSourcesRefreshedForUnitTests();

        updateGraph.completeCycleForUnitTests();

        final Table filter1 = future1.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
        TstUtils.assertTableEquals(testUpdate, filter1);
        TstUtils.assertTableEquals(filter2, filter1);
    }

    public void testSort() throws ExecutionException, InterruptedException, TimeoutException {
        testSortInternal(false);
    }

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

    public void testChain() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"), col("z", true, false, true));
        final Table tableStart = TstUtils.testRefreshingTable(i(1, 3).toTracking(),
                col("x", 3, 1), col("y", "c", "a"), col("z", true, true), col("u", 12, 4));

        final Table tableUpdate = TstUtils.testRefreshingTable(i(1, 2, 4).toTracking(),
                col("x", 4, 3, 1), col("y", "d", "c", "a"), col("z", true, true, true), col("u", 16, 12, 4));

        final Callable<Table> callable = () -> table.updateView("u=x*4").where("z").sortDescending("x");

        updateGraph.startCycleForUnitTests(false);

        final Table chain1 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(chain1, tableStart);

        TstUtils.addToTable(table, i(3), col("x", 4), col("y", "d"), col("z", true));

        final Table chain2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(chain1));
        TstUtils.assertTableEquals(tableStart, prevTable(chain2));

        table.notifyListeners(i(3), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table chain3 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        showWithRowSet(chain3);

        TstUtils.assertTableEquals(tableStart, prevTable(chain1));
        TstUtils.assertTableEquals(tableStart, prevTable(chain2));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, chain1);
        TstUtils.assertTableEquals(tableUpdate, chain2);
        TstUtils.assertTableEquals(tableUpdate, chain3);
    }

    public void testIterative() {
        final List<Function<Table, Table>> transformations = new ArrayList<>();
        transformations.add(t -> t.updateView("i4=intCol * 4"));
        transformations.add(t -> t.where("boolCol"));
        transformations.add(t -> t.where("boolCol2"));
        transformations.add(t -> t.sortDescending("doubleCol"));
        transformations.add(Table::flatten);

        testIterative(transformations, 0, new MutableInt(50));
    }

    public void testIterativeQuickFilter() {
        final List<Function<Table, Table>> transformations = new ArrayList<>();
        transformations.add(t -> t.where("boolCol2"));
        transformations.add(t -> t.where(DisjunctiveFilter.makeDisjunctiveFilter(
                WhereFilterFactory.expandQuickFilter(t.getDefinition(), "10", QuickFilterMode.NORMAL))));
        transformations.add(t -> t.sortDescending("doubleCol"));
        transformations.add(Table::flatten);
        testIterative(transformations);
    }

    public void testIterativeDisjunctiveCondition() {
        final List<Function<Table, Table>> transformations = new ArrayList<>();
        transformations.add(
                t -> t.where(DisjunctiveFilter.makeDisjunctiveFilter(
                        ConditionFilter.createConditionFilter("false"),
                        ConditionFilter.createConditionFilter("true"))));
        testIterative(transformations);
    }

    private void testIterative(List<Function<Table, Table>> transformations) {
        testIterative(transformations, 0, new MutableInt(50));
    }

    @SuppressWarnings("ConstantConditions")
    private void testIterative(List<Function<Table, Table>> transformations, int seed, MutableInt numSteps) {
        final ColumnInfo<?, ?>[] columnInfos;

        final int size = 100;
        final Random random = new Random(seed);
        final int maxSteps = numSteps.intValue();

        final QueryTable table = getTable(size, random,
                columnInfos = initColumnInfos(new String[] {"Sym", "intCol", "boolCol", "boolCol2", "doubleCol"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd", "ee", "ff", "gg", "hh", "ii"),
                        new IntGenerator(0, 100),
                        new BooleanGenerator(),
                        new BooleanGenerator(),
                        new DoubleGenerator(0, 100)));


        final Callable<Table> complete = () -> {
            Table t = table;
            for (Function<Table, Table> transformation : transformations) {
                t = transformation.apply(t);
            }
            return t;
        };

        final List<Pair<Callable<Table>, Function<Table, Table>>> splitCallables = new ArrayList<>();
        for (int ii = 1; ii <= transformations.size() - 1; ++ii) {
            final int fii = ii;
            final Callable<Table> firstHalf = () -> {
                Table t = table;
                for (int jj = 0; jj < fii; ++jj) {
                    t = transformations.get(jj).apply(t);
                }
                return t;
            };
            final Function<Table, Table> secondHalf = (firstResult) -> {
                Table t = firstResult;
                for (int jj = fii; jj < transformations.size(); ++jj) {
                    t = transformations.get(jj).apply(t);
                }
                return t;
            };
            splitCallables.add(new Pair<>(firstHalf, secondHalf));
        }

        final Table standard = updateGraph.exclusiveLock().computeLocked(() -> {
            try {
                return complete.call();
            } catch (Exception e) {
                e.printStackTrace();
                TestCase.fail(e.getMessage());
                throw new RuntimeException(e);
            }
        });

        final boolean beforeUpdate = true;
        final boolean beforeNotify = true;

        final boolean beforeAndAfterUpdate = true;
        final boolean beforeStartAndBeforeUpdate = true;
        final boolean beforeStartAndAfterUpdate = true;

        final boolean beforeAndAfterNotify = true;
        final boolean beforeStartAndAfterNotify = true;
        final boolean beforeUpdateAndAfterNotify = true;

        final boolean beforeAndAfterCycle = true;
        final boolean beforeUpdateAndAfterCycle = true;
        final boolean beforeNotifyAndAfterCycle = true;
        final boolean beforeStartAndAfterCycle = true;

        final List<Table> results = new ArrayList<>();
        // noinspection MismatchedQueryAndUpdateOfCollection
        final List<TableUpdateListener> listeners = new ArrayList<>();
        int lastResultSize = 0;

        try {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Input Table:\n");
                showWithRowSet(table);
            }

            for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
                final int i = numSteps.intValue();
                if (RefreshingTableTestCase.printTableUpdates) {
                    System.out.println("Step = " + i);
                }

                final List<Table> beforeStartFirstHalf = new ArrayList<>(splitCallables.size());
                for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                    beforeStartFirstHalf.add(pool.submit(splitCallable.first).get(TIMEOUT_LENGTH, TIMEOUT_UNIT));
                }

                updateGraph.startCycleForUnitTests(false);

                if (beforeUpdate) {
                    // before we update the underlying data
                    final Table chain1 = pool.submit(complete).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                            .withAttributes(Map.of(
                                    "Step", i,
                                    "Type", "beforeUpdate"));
                    results.add(chain1);
                }

                if (beforeStartAndBeforeUpdate) {
                    final List<Table> beforeStartAndBeforeUpdateSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeStartFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeUpdateSplit"));
                        beforeStartAndBeforeUpdateSplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndBeforeUpdateSplitResults);
                }

                final List<Table> beforeUpdateFirstHalf = new ArrayList<>(splitCallables.size());
                for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                    beforeUpdateFirstHalf.add(pool.submit(splitCallable.first).get(TIMEOUT_LENGTH, TIMEOUT_UNIT));
                }

                final RowSet[] updates = GenerateTableUpdates.computeTableUpdates(size, random, table, columnInfos);

                if (beforeNotify) {
                    // after we update the underlying data, but before we notify
                    final Table chain2 = pool.submit(complete).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                            .withAttributes(Map.of(
                                    "Step", i,
                                    "Type", "beforeNotify"));
                    results.add(chain2);
                }

                if (beforeAndAfterUpdate) {
                    final List<Table> beforeAndAfterUpdateSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeUpdateFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeAndAfterUpdateSplit",
                                        "SplitIndex", splitIndex));
                        beforeAndAfterUpdateSplitResults.add(splitResult);
                    }
                    results.addAll(beforeAndAfterUpdateSplitResults);
                }

                if (beforeStartAndAfterUpdate) {
                    final List<Table> beforeStartAndAfterUpdateSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeStartFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeStartAndAfterUpdate",
                                        "SplitIndex", splitIndex));
                        beforeStartAndAfterUpdateSplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndAfterUpdateSplitResults);
                }

                final List<Table> beforeNotifyFirstHalf = new ArrayList<>(splitCallables.size());
                for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                    beforeNotifyFirstHalf.add(pool.submit(splitCallable.first).get(TIMEOUT_LENGTH, TIMEOUT_UNIT));
                }

                table.notifyListeners(updates[0], updates[1], updates[2]);
                updateGraph.markSourcesRefreshedForUnitTests();

                if (beforeAndAfterNotify) {
                    final List<Table> beforeAndAfterNotifySplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeNotifyFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeAndAfterNotify"));
                        beforeAndAfterNotifySplitResults.add(splitResult);
                    }
                    results.addAll(beforeAndAfterNotifySplitResults);
                }

                if (beforeStartAndAfterNotify) {
                    final List<Table> beforeStartAndAfterNotifySplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeStartFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeStartAndAfterNotify",
                                        "SplitIndex", splitIndex));
                        beforeStartAndAfterNotifySplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndAfterNotifySplitResults);
                }

                if (beforeUpdateAndAfterNotify) {
                    final List<Table> beforeUpdateAndAfterNotifySplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeUpdateFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeUpdateAndAfterNotify",
                                        "SplitIndex", splitIndex));
                        beforeUpdateAndAfterNotifySplitResults.add(splitResult);
                    }
                    results.addAll(beforeUpdateAndAfterNotifySplitResults);
                }

                final List<Table> beforeCycleFirstHalf = new ArrayList<>(splitCallables.size());
                if (beforeAndAfterCycle) {
                    for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                        beforeCycleFirstHalf.add(pool.submit(splitCallable.first).get(TIMEOUT_LENGTH, TIMEOUT_UNIT));
                    }
                }

                if (beforeNotify) {
                    // after notification, on the same cycle
                    final Table chain3 = pool.submit(complete).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                            .withAttributes(Map.of(
                                    "Step", i,
                                    "Type", "beforeNotify"));
                    results.add(chain3);
                }

                for (int newResult = lastResultSize; newResult < results.size(); ++newResult) {
                    final Table dynamicTable = results.get(newResult);
                    final InstrumentedTableUpdateListenerAdapter listener =
                            new InstrumentedTableUpdateListenerAdapter("errorListener", dynamicTable, false) {
                                @Override
                                public void onUpdate(final TableUpdate upstream) {}

                                @Override
                                public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                                    originalException.printStackTrace(System.err);
                                    TestCase.fail(originalException.getMessage());
                                }
                            };
                    listeners.add(listener);
                    dynamicTable.addUpdateListener(listener);
                }
                lastResultSize = results.size();
                updateGraph
                        .completeCycleForUnitTests();

                if (beforeStartAndAfterCycle) {
                    final List<Table> beforeStartAndAfterCycleSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeStartFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeStartAndAfterCycle",
                                        "SplitIndex", splitIndex));
                        beforeStartAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = updateGraph.exclusiveLock()
                                .computeLocked(() -> splitCallables.get(fSplitIndex).second
                                        .apply(beforeStartFirstHalf.get(fSplitIndex)))
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeStartAndAfterCycleLocked",
                                        "SplitIndex", splitIndex));
                        beforeStartAndAfterCycleSplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndAfterCycleSplitResults);
                }

                if (beforeUpdateAndAfterCycle) {
                    final List<Table> beforeUpdateAndAfterCycleSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeUpdateFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeUpdateAndAfterCycle",
                                        "SplitIndex", splitIndex));
                        beforeUpdateAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = updateGraph.exclusiveLock()
                                .computeLocked(() -> splitCallables.get(fSplitIndex).second
                                        .apply(beforeUpdateFirstHalf.get(fSplitIndex)))
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeUpdateAndAfterCycleLocked",
                                        "SplitIndex", splitIndex));
                        beforeUpdateAndAfterCycleSplitResults.add(splitResult);
                    }
                    results.addAll(beforeUpdateAndAfterCycleSplitResults);
                }

                if (beforeNotifyAndAfterCycle) {
                    final List<Table> beforeNotifyAndAfterCycleSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeNotifyFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeNotifyAndAfterCycle",
                                        "SplitIndex", splitIndex));
                        beforeNotifyAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final Table splitResult =
                                splitCallables.get(splitIndex).second.apply(beforeNotifyFirstHalf.get(splitIndex))
                                        .withAttributes(Map.of(
                                                "Step", i,
                                                "Type", "beforeNotifyAndAfterCycleLocked",
                                                "SplitIndex", splitIndex));
                        beforeNotifyAndAfterCycleSplitResults.add(splitResult);
                    }

                    results.addAll(beforeNotifyAndAfterCycleSplitResults);
                }

                Assert.eqTrue(beforeAndAfterCycle, "beforeAndAfterCycle");
                if (transformations.size() > 1) {
                    Assert.eqFalse(beforeCycleFirstHalf.isEmpty(), "beforeCycleFirstHalf.isEmpty()");
                    final List<Table> beforeAndAfterCycleSplitResults = new ArrayList<>(splitCallables.size());

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeCycleFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeAndAfterCycle",
                                        "SplitIndex", splitIndex));
                        beforeAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = updateGraph.exclusiveLock()
                                .computeLocked(() -> splitCallables.get(fSplitIndex).second
                                        .apply(beforeCycleFirstHalf.get(fSplitIndex)))
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeAndAfterCycle",
                                        "SplitIndex", splitIndex));
                        beforeAndAfterCycleSplitResults.add(splitResult);
                    }

                    results.addAll(beforeAndAfterCycleSplitResults);
                }

                if (RefreshingTableTestCase.printTableUpdates) {
                    System.out.println("Input Table: (" + Objects.hashCode(table) + ")");
                    showWithRowSet(table);
                    System.out.println("Standard Table: (" + Objects.hashCode(standard) + ")");
                    showWithRowSet(standard);
                    System.out.println("Verifying " + results.size() + " tables (size = " + standard.size() + ")");
                }

                // now verify all the outstanding results
                for (Table checkTable : results) {
                    String diff = diff(checkTable, standard, 10);
                    if (!diff.isEmpty() && RefreshingTableTestCase.printTableUpdates) {
                        System.out.println("Check Table: " + checkTable.getAttribute("Step") + ", " +
                                checkTable.getAttribute("Type") +
                                ", splitIndex=" + checkTable.getAttribute("SplitIndex") +
                                ", hash=" + Objects.hashCode(checkTable));
                        showWithRowSet(checkTable);
                    }
                    TestCase.assertEquals("", diff);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            TestCase.fail(e.getMessage());
        }
    }

    public void testSelectDistinct() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6, 8).toTracking(),
                col("y", "a", "b", "a", "c"));
        final Table expected1 = newTable(col("y", "a", "b", "c"));
        final Table expected2 = newTable(col("y", "a", "d", "b", "c"));
        final Table expected2outOfOrder = newTable(col("y", "a", "b", "c", "d"));

        updateGraph.startCycleForUnitTests(false);

        final Callable<Table> callable = () -> table.selectDistinct("y");

        final Table distinct1 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(expected1, distinct1);

        TstUtils.addToTable(table, i(3), col("y", "d"));

        TstUtils.assertTableEquals(expected1, distinct1);

        final Table distinct2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(expected1, distinct2);
        TstUtils.assertTableEquals(expected1, prevTable(distinct2));

        table.notifyListeners(i(3), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table distinct3 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(expected1, prevTable(distinct1));
        TstUtils.assertTableEquals(expected1, prevTable(distinct2));
        TstUtils.assertTableEquals(expected2, distinct3);
        TstUtils.assertTableEquals(expected2, prevTable(distinct3));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(expected2outOfOrder, distinct1);
        TstUtils.assertTableEquals(expected2outOfOrder, distinct2);
        TstUtils.assertTableEquals(expected2, distinct3);
        TstUtils.assertTableEquals(expected2, prevTable(distinct3));
    }

    @ReflexiveUse(referrers = "io.deephaven.engine.table.impl.TestConcurrentInstantiation")
    public static String identitySleep(String x) {
        SleepUtil.sleep(50);
        return x;
    }

    public static class BarrierFunction implements UnaryOperator<String> {
        final AtomicInteger invocationCount = new AtomicInteger(0);
        int sleepDuration = 50;

        @Override
        public String apply(String s) {
            synchronized (invocationCount) {
                invocationCount.incrementAndGet();
                invocationCount.notifyAll();
            }
            if (sleepDuration > 0) {
                SleepUtil.sleep(sleepDuration);
            }
            return s;
        }

        void waitForInvocation(int count, int timeoutMillis) throws InterruptedException {
            final long endTime = System.currentTimeMillis() + timeoutMillis;
            synchronized (invocationCount) {
                long now = System.currentTimeMillis();
                while (invocationCount.get() < count && now < endTime) {
                    invocationCount.wait(endTime - now);
                    now = System.currentTimeMillis();
                }
                if (invocationCount.get() < count) {
                    throw new RuntimeException("Invocation count did not advance.");
                }
            }
        }
    }

    public void testSelectDistinctReset() throws ExecutionException, InterruptedException, TimeoutException {
        final BarrierFunction barrierFunction = new BarrierFunction();
        QueryScope.addParam("barrierFunction", barrierFunction);

        try {
            final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6, 8).toTracking(),
                    col("y", "a", "b", "a", "c"));
            final Table slowed = table.updateView("z=barrierFunction.apply(y)");
            final Table expected1 = newTable(col("z", "a", "b"));

            updateGraph.startCycleForUnitTests(false);

            final Callable<Table> callable = () -> slowed.selectDistinct("z");

            final Future<Table> future1 = pool.submit(callable);
            barrierFunction.waitForInvocation(2, 5000);

            System.out.println("Removing rows");
            removeRows(table, i(8));
            table.notifyListeners(i(), i(8), i());
            updateGraph.markSourcesRefreshedForUnitTests();

            barrierFunction.sleepDuration = 0;

            updateGraph.completeCycleForUnitTests();

            final Table distinct1 = future1.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);
            TstUtils.assertTableEquals(expected1, distinct1);
        } finally {
            QueryScope.addParam("barrierFunction", null);
        }
    }

    public void testSumBy() throws Exception {
        testByConcurrent(t -> t.sumBy("KeyColumn"));
        testByConcurrent(t -> t.absSumBy("KeyColumn"));
    }

    public void testAvgBy() throws Exception {
        testByConcurrent(t -> t.avgBy("KeyColumn"));
    }

    public void testVarBy() throws Exception {
        testByConcurrent(t -> t.varBy("KeyColumn"));
    }

    public void testStdBy() throws Exception {
        testByConcurrent(t -> t.varBy("KeyColumn"));
    }

    public void testCountBy() throws Exception {
        testByConcurrent(t -> t.varBy("KeyColumn"));
    }

    private static <T extends Table> T setAddOnly(@NotNull final T table) {
        // noinspection unchecked
        return (T) table.withAttributes(Map.of(Table.ADD_ONLY_TABLE_ATTRIBUTE, true));
    }

    public void testMinMaxBy() throws Exception {
        testByConcurrent(t -> t.maxBy("KeyColumn"));
        testByConcurrent(t -> t.minBy("KeyColumn"));
        testByConcurrent(t -> setAddOnly(t).minBy("KeyColumn"), true, false, false, true);
        testByConcurrent(t -> setAddOnly(t).maxBy("KeyColumn"), true, false, false, true);
    }

    public void testFirstLastBy() throws Exception {
        testByConcurrent(t -> t.firstBy("KeyColumn"));
        testByConcurrent(t -> t.lastBy("KeyColumn"));
    }

    public void testSortedFirstLastBy() throws Exception {
        testByConcurrent(t -> SortedBy.sortedFirstBy(t, "IntCol", "KeyColumn"));
        testByConcurrent(t -> SortedBy.sortedLastBy(t, "IntCol", "KeyColumn"));
    }

    public void testKeyedBy() throws Exception {
        testByConcurrent(t -> t.groupBy("KeyColumn"));
    }

    public void testNoKeyBy() throws Exception {
        testByConcurrent(Table::groupBy, false, false, true, true);
    }

    public void testPercentileBy() throws Exception {
        final Function<Table, String[]> nonKeyColumnNames = t -> t.getDefinition().getColumnStream()
                .map(ColumnDefinition::getName).filter(cn -> !cn.equals("KeyColumn")).toArray(String[]::new);
        testByConcurrent(t -> t.dropColumns("KeyColumn").aggBy(AggPct(0.25, nonKeyColumnNames.apply(t))),
                false, false, true, true);
        testByConcurrent(t -> t.dropColumns("KeyColumn").aggBy(AggPct(0.75, nonKeyColumnNames.apply(t))),
                false, false, true, true);
        testByConcurrent(t -> t.medianBy("KeyColumn"));
    }

    public void testAggCombo() throws Exception {
        testByConcurrent(t -> t.aggBy(List.of(AggAvg("AvgInt=IntCol"), AggCount("NumInts"),
                AggSum("SumDouble=DoubleCol"), AggMax("MaxDouble=DoubleCol")), "KeyColumn"));
    }

    public void testWavgBy() throws Exception {
        testByConcurrent(t -> t.wavgBy("IntCol", "KeyColumn"), true, true, true, false);
        testByConcurrent(t -> t.wavgBy("IntCol", "KeyColumn"), true, false, true, false);
        testByConcurrent(t -> t.wavgBy("DoubleCol", "KeyColumn"), true, true, true, false);
        testByConcurrent(t -> t.wavgBy("DoubleCol", "KeyColumn"), true, false, true, false);
    }

    private void testByConcurrent(Function<Table, Table> function) throws Exception {
        testByConcurrent(function, true, false, true, true);
        testByConcurrent(function, true, true, true, true);
    }

    private void testByConcurrent(Function<Table, Table> function, boolean hasKeys, boolean withReset,
            boolean allowModifications, boolean haveBigNumerics) throws Exception {
        setExpectError(false);

        final QueryTable table = makeByConcurrentBaseTable(haveBigNumerics);
        final QueryTable table2 = makeByConcurrentStep2Table(allowModifications, haveBigNumerics);

        final BarrierFunction barrierFunction = withReset ? new BarrierFunction() : null;
        QueryScope.addParam("barrierFunction", barrierFunction);

        try {
            final Callable<Table> callable;
            final Table slowed;
            if (withReset) {
                ExecutionContext.getContext().getQueryLibrary().importStatic(TestConcurrentInstantiation.class);

                slowed = table.updateView("KeyColumn=barrierFunction.apply(KeyColumn)");
                callable = () -> {
                    final long start = System.currentTimeMillis();
                    System.out.println("Applying callable to slowed table.");
                    try {
                        return function.apply(slowed);
                    } finally {
                        System.out.println("Callable complete: " + (System.currentTimeMillis() - start));
                    }
                };
            } else {
                slowed = null;
                callable = () -> function.apply(table);
            }

            // We only care about the silent version of this table, as it's just a vessel to tick and ensure that the
            // resultant table is computed using the appropriate version.
            final Table expected1 = updateGraph.exclusiveLock().computeLocked(
                    () -> function.apply(table.silent()).select());
            final Table expected2 = updateGraph.exclusiveLock()
                    .computeLocked(() -> function.apply(table2));

            updateGraph.startCycleForUnitTests(false);

            final Future<Table> future1 = pool.submit(callable);
            final Table result1;
            if (withReset) {
                barrierFunction.waitForInvocation(2, 5000);
            }
            result1 = future1.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

            System.out.println("Result 1");
            TableTools.show(result1);
            System.out.println("Expected 1");
            TableTools.show(expected1);

            TstUtils.assertTableEquals(expected1, result1, TableDiff.DiffItems.DoublesExact);

            doByConcurrentAdditions(table, haveBigNumerics);
            if (allowModifications) {
                doByConcurrentModifications(table, haveBigNumerics);
            }
            final Table prevResult1a = prevTable(result1);

            System.out.println("PrevResulta 1");
            TableTools.show(prevResult1a);
            System.out.println("Expected 1");
            TableTools.show(expected1);

            TstUtils.assertTableEquals(expected1, prevResult1a, TableDiff.DiffItems.DoublesExact);

            final Table result2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

            System.out.println("Result 2");
            TableTools.show(result2);
            System.out.println("Expected 1");
            TableTools.show(expected1);

            // The column sources are redirected, and the underlying table has been updated without a notification
            // _yet_,
            // so the column sources have _already_ changed and we are inside an update cycle, so the value of get() is
            // indeterminate
            // therefore this assert is not really a valid thing to do.
            // TstUtils.assertTableEquals(expected1, result2);
            final Table prevResult2a = prevTable(result2);
            System.out.println("Prev Result 2a");
            TableTools.show(prevResult2a);

            TstUtils.assertTableEquals(expected1, prevResult2a, TableDiff.DiffItems.DoublesExact);

            table.notifyListeners(i(5, 9), i(), allowModifications ? i(8) : i());
            updateGraph.markSourcesRefreshedForUnitTests();

            final Future<Table> future3 = pool.submit(callable);
            if (withReset) {
                while (((QueryTable) slowed).getLastNotificationStep() != updateGraph.clock().currentStep()) {
                    updateGraph.flushOneNotificationForUnitTests();
                }
            }
            final Table result3 = future3.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

            System.out.println("Prev Result 1");
            final Table prevResult1b = prevTable(result1);
            TableTools.show(prevResult1b);
            TstUtils.assertTableEquals(expected1, prevResult1b, TableDiff.DiffItems.DoublesExact);

            System.out.println("Prev Result 2b");
            final Table prevResult2b = prevTable(result2);
            TableTools.show(prevResult2b);
            TstUtils.assertTableEquals(expected1, prevResult2b, TableDiff.DiffItems.DoublesExact);

            System.out.println("Result 3");
            TableTools.show(result3);
            System.out.println("Expected 2");
            TableTools.show(expected2);
            TstUtils.assertTableEquals(expected2, result3, TableDiff.DiffItems.DoublesExact);

            updateGraph.completeCycleForUnitTests();

            if (hasKeys) {
                TstUtils.assertTableEquals(expected2.sort("KeyColumn"), result1.sort("KeyColumn"),
                        TableDiff.DiffItems.DoublesExact);
                TstUtils.assertTableEquals(expected2.sort("KeyColumn"), result2.sort("KeyColumn"),
                        TableDiff.DiffItems.DoublesExact);
            } else {
                TstUtils.assertTableEquals(expected2, result1, TableDiff.DiffItems.DoublesExact);
                TstUtils.assertTableEquals(expected2, result2, TableDiff.DiffItems.DoublesExact);
            }
            TstUtils.assertTableEquals(expected2, result3, TableDiff.DiffItems.DoublesExact);

        } finally {
            QueryScope.addParam("barrierFunction", null);
        }
    }

    public void testPartitionByConcurrent() throws Exception {
        testPartitionByConcurrent(false);
        testPartitionByConcurrent(true);
    }

    private void testPartitionByConcurrent(boolean withReset) throws Exception {
        setExpectError(false);

        final QueryTable table = makeByConcurrentBaseTable(false);
        final QueryTable table2 = makeByConcurrentStep2Table(true, false);


        final Callable<PartitionedTable> callable;
        final Table slowed;
        if (withReset) {
            ExecutionContext.getContext().getQueryLibrary().importStatic(TestConcurrentInstantiation.class);

            slowed = table.updateView("KeyColumn=identitySleep(KeyColumn)");
            callable = () -> slowed.partitionBy("KeyColumn");
        } else {
            slowed = null;
            callable = () -> table.partitionBy("KeyColumn");
        }

        // We only care about the silent version of this table, as it's just a vessel to tick and ensure that the
        // resultant table
        // is computed using the appropriate version.
        final Table expected1 = updateGraph.exclusiveLock().computeLocked(
                () -> table.silent().partitionBy("KeyColumn").merge().select());
        final Table expected2 = updateGraph.exclusiveLock().computeLocked(
                () -> table2.silent().partitionBy("KeyColumn").merge().select());

        updateGraph.startCycleForUnitTests(false);

        final Future<PartitionedTable> future1 = pool.submit(callable);
        final PartitionedTable result1;
        if (withReset) {
            SleepUtil.sleep(25);
        }
        result1 = future1.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        System.out.println("Result 1");
        final Table result1a = result1.constituentFor("a");
        final Table result1b = result1.constituentFor("b");
        final Table result1c = result1.constituentFor("c");
        TableTools.show(result1a);
        TableTools.show(result1b);
        TableTools.show(result1c);
        System.out.println("Expected 1");
        TableTools.show(expected1);

        TstUtils.assertTableEquals(expected1.where("KeyColumn = `a`"), result1a);
        TstUtils.assertTableEquals(expected1.where("KeyColumn = `b`"), result1b);
        TstUtils.assertTableEquals(expected1.where("KeyColumn = `c`"), result1c);

        doByConcurrentAdditions(table, false);
        doByConcurrentModifications(table, false);

        final PartitionedTable result2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        System.out.println("Result 2");
        final Table result2a = result2.constituentFor("a");
        final Table result2b = result2.constituentFor("b");
        final Table result2c = result2.constituentFor("c");
        final Table result2d_1 = result2.constituentFor("d");
        assertNull(result2d_1);

        TableTools.show(result2a);
        TableTools.show(result2b);
        TableTools.show(result2c);
        System.out.println("Expected 1");
        TableTools.show(expected1);

        table.notifyListeners(i(5, 9), i(), i(8));
        updateGraph.markSourcesRefreshedForUnitTests();

        final Future<PartitionedTable> future3 = pool.submit(callable);
        if (withReset) {
            while (((QueryTable) slowed).getLastNotificationStep() != updateGraph.clock().currentStep()) {
                updateGraph.flushOneNotificationForUnitTests();
            }
        }
        final PartitionedTable result3 = future3.get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        System.out.println("Result 3");
        final Table result3a = result3.constituentFor("a");
        final Table result3b = result3.constituentFor("b");
        final Table result3c = result3.constituentFor("c");
        final Table result3d = result3.constituentFor("d");

        System.out.println("Expected 2");
        TableTools.show(expected2);

        TstUtils.assertTableEquals(expected2.where("KeyColumn = `a`"), result3a);
        TstUtils.assertTableEquals(expected2.where("KeyColumn = `b`"), result3b);
        assertNull(result3c);
        TstUtils.assertTableEquals(expected2.where("KeyColumn = `d`"), result3d);

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(expected2, result1.merge());
        TstUtils.assertTableEquals(expected2, result2.merge());
        TstUtils.assertTableEquals(expected2, result3.merge());
    }

    private QueryTable makeByConcurrentBaseTable(boolean haveBigNumerics) {
        final List<ColumnHolder<?>> columnHolders = new ArrayList<>(Arrays.asList(
                col("KeyColumn", "a", "b", "a", "c"),
                intCol("IntCol", 1, 2, 3, 4),
                doubleCol("DoubleCol", 100.1, 200.2, 300.3, 400.4),
                floatCol("FloatCol", 10.1f, 20.2f, 30.3f, 40.4f),
                shortCol("ShortCol", (short) 10, (short) 20, (short) 30, (short) 40),
                byteCol("ByteCol", (byte) 11, (byte) 12, (byte) 13, (byte) 14),
                charCol("CharCol", 'A', 'B', 'C', 'D'),
                longCol("LongCol", 10_000_000_000L, 20_000_000_000L, 30_000_000_000L, 40_000_000_000L)));

        if (haveBigNumerics) {
            columnHolders.add(col("BigDecCol", BigDecimal.valueOf(10000.1), BigDecimal.valueOf(20000.2),
                    BigDecimal.valueOf(40000.3), BigDecimal.valueOf(40000.4)));
            columnHolders.add(col("BigIntCol", BigInteger.valueOf(100000), BigInteger.valueOf(200000),
                    BigInteger.valueOf(300000), BigInteger.valueOf(400000)));
        }


        return TstUtils.testRefreshingTable(i(2, 4, 6, 8).toTracking(),
                columnHolders.toArray(ColumnHolder.ZERO_LENGTH_COLUMN_HOLDER_ARRAY));
    }

    private QueryTable makeByConcurrentStep2Table(boolean allowModifications, boolean haveBigNumerics) {
        final QueryTable table2 = makeByConcurrentBaseTable(haveBigNumerics);
        doByConcurrentAdditions(table2, haveBigNumerics);
        if (allowModifications) {
            doByConcurrentModifications(table2, haveBigNumerics);
        }
        return table2;

    }

    private void doByConcurrentModifications(QueryTable table, boolean haveBigNumerics) {
        final List<ColumnHolder<?>> columnHolders = new ArrayList<>(Arrays.asList(
                col("KeyColumn", "b"),
                intCol("IntCol", 7),
                doubleCol("DoubleCol", 700.7),
                floatCol("FloatCol", 70.7f),
                shortCol("ShortCol", (short) 70),
                byteCol("ByteCol", (byte) 17),
                charCol("CharCol", 'E'),
                longCol("LongCol", 70_000_000_000L)));
        if (haveBigNumerics) {
            columnHolders.addAll(Arrays.asList(
                    col("BigDecCol", BigDecimal.valueOf(70000.7)),
                    col("BigIntCol", BigInteger.valueOf(700000))));
        }

        TstUtils.addToTable(table, i(8), columnHolders.toArray(ColumnHolder.ZERO_LENGTH_COLUMN_HOLDER_ARRAY));
    }

    private void doByConcurrentAdditions(QueryTable table, boolean haveBigNumerics) {

        final List<ColumnHolder<?>> columnHolders = new ArrayList<>(Arrays.asList(
                col("KeyColumn", "d", "a"),
                intCol("IntCol", 5, 6),
                doubleCol("DoubleCol", 505.5, 600.6),
                floatCol("FloatCol", 50.5f, 60.6f),
                shortCol("ShortCol", (short) 50, (short) 60),
                byteCol("ByteCol", (byte) 15, (byte) 16),
                charCol("CharCol", 'E', 'F'),
                longCol("LongCol", 50_000_000_000L, 60_000_000_000L)));

        if (haveBigNumerics) {
            columnHolders.addAll(Arrays.asList(
                    col("BigDecCol", BigDecimal.valueOf(50000.5), BigDecimal.valueOf(60000.6)),
                    col("BigIntCol", BigInteger.valueOf(500000), BigInteger.valueOf(600000))));
        }

        TstUtils.addToTable(table, i(5, 9), columnHolders.toArray(ColumnHolder.ZERO_LENGTH_COLUMN_HOLDER_ARRAY));
    }

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
}
