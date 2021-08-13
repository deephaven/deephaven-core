package io.deephaven.db.v2;

import io.deephaven.base.Pair;
import io.deephaven.base.SleepUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.MatchPairFactory;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.select.SelectFilterFactory;
import io.deephaven.db.tables.utils.TableDiff;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.v2.by.*;
import io.deephaven.db.v2.remote.ConstructSnapshot;
import io.deephaven.db.v2.select.ConditionFilter;
import io.deephaven.db.v2.select.DisjunctiveFilter;
import io.deephaven.db.v2.select.DynamicWhereFilter;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.utils.ColumnHolder;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.gui.table.QuickFilterMode;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReflexiveUse;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.v2.TstUtils.*;
import static io.deephaven.db.v2.by.ComboAggregateFactory.*;
import static io.deephaven.util.QueryConstants.NULL_INT;

@Category(OutOfBandTest.class)
public class TestConcurrentInstantiation extends QueryTableTestBase {
    private final ExecutorService pool = Executors.newFixedThreadPool(1);
    private final ExecutorService dualPool = Executors.newFixedThreadPool(2);

    public void testTreeTableFilter() throws ExecutionException, InterruptedException {
        final QueryTable source = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(10),
                col("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                col("Parent", NULL_INT, NULL_INT, 1, 1, 2, 3, 5, 5, 3, 2));
        final Table treed = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> source.treeTable("Sentinel", "Parent"));

        final Callable<Table> callable = () -> TreeTableFilter.rawFilterTree(treed, "Sentinel in 4, 6, 9, 11, 12, 13, 14, 15");

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        final Table rawSorted = pool.submit(callable).get();
        TableTools.show(rawSorted);

        assertTrue(Arrays.equals(new int[]{1, 3, 4, 6, 9}, (int[]) rawSorted.getColumn("Sentinel").getDirect()));

        TstUtils.addToTable(source, i(10), c("Sentinel", 11),
                c("Parent", 2));
        final Table table2 = pool.submit(callable).get();
        assertEquals(TableTools.diff(rawSorted, table2, 20), "");

        source.notifyListeners(i(10), i(), i());

        final Future<Table> future3 = pool.submit(callable);
        assertEquals(TableTools.diff(rawSorted, table2, 20), "");

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
        final Table table3 = future3.get();

        assertEquals(TableTools.diff(rawSorted, table2, 20), "");
        assertEquals(TableTools.diff(table2, table3, 20), "");

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        TstUtils.addToTable(source, i(11), c("Sentinel", 12), c("Parent", 10));

        final Table table4 = pool.submit(callable).get();
        assertEquals(TableTools.diff(rawSorted, table2, 20), "");
        assertEquals(TableTools.diff(table2, table3, 20), "");
        assertEquals(TableTools.diff(table3, table4, 20), "");

        source.notifyListeners(i(11), i(), i());
        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        assertTrue(Arrays.equals(new int[]{1, 2, 3, 4, 6, 9, 10, 11, 12}, (int[]) rawSorted.getColumn("Sentinel").getDirect()));
        assertEquals(TableTools.diff(rawSorted, table2, 20), "");
        assertEquals(TableTools.diff(table2, table3, 20), "");
        assertEquals(TableTools.diff(table3, table4, 20), "");

    }


    public void testFlatten() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"));

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Table flat = pool.submit(table::flatten).get();

        TestCase.assertEquals(TableTools.diff(flat, table, 10), "");
        TestCase.assertEquals(TableTools.diff(flat, tableStart, 10), "");

        TstUtils.addToTable(table, i(3), c("x", 4), c("y", "d"));

        final Table flat2 = pool.submit(table::flatten).get();

        TstUtils.assertTableEquals(prevTable(flat), tableStart);
        TstUtils.assertTableEquals(prevTable(flat2), tableStart);

        table.notifyListeners(i(3), i(), i());

        final Table flat3 = pool.submit(table::flatten).get();

        TstUtils.assertTableEquals(prevTable(flat), tableStart);
        TstUtils.assertTableEquals(prevTable(flat2), tableStart);

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TstUtils.assertTableEquals(table, flat);
        TstUtils.assertTableEquals(table, flat2);
        TstUtils.assertTableEquals(table, flat3);
    }

    public void testUpdateView() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"), c("z", 4, 8, 12));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(2, 3, 4, 6), c("x", 1, 4, 2, 3), c("y", "a", "d", "b", "c"), c("z", 4, 16, 8, 12));

        final Callable<Table> callable = () -> table.updateView("z=x*4");

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Table updateView1 = pool.submit(callable).get();

        TestCase.assertEquals(TableTools.diff(updateView1, tableStart, 10), "");

        TstUtils.addToTable(table, i(3), c("x", 4), c("y", "d"));

        final Table updateView2 = pool.submit(callable).get();

        TstUtils.assertTableEquals(tableStart, prevTable(updateView1));
        TstUtils.assertTableEquals(tableStart, prevTable(updateView2));

        table.notifyListeners(i(3), i(), i());

        final Table updateView3 = pool.submit(callable).get();

        TstUtils.assertTableEquals(tableStart, prevTable(updateView1));
        TstUtils.assertTableEquals(tableStart, prevTable(updateView2));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, updateView1);
        TstUtils.assertTableEquals(tableUpdate, updateView2);
        TstUtils.assertTableEquals(tableUpdate, updateView3);
    }

    public void testView() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 4, 6), c("y", "a", "b", "c"), c("z", 4, 8, 12));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(2, 3, 4, 6), c("y", "a", "d", "b", "c"), c("z", 4, 16, 8, 12));

        final Callable<Table> callable = () -> table.view("y", "z=x*4");

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Table updateView1 = pool.submit(callable).get();

        TestCase.assertEquals(TableTools.diff(updateView1, tableStart, 10), "");

        TstUtils.addToTable(table, i(3), c("x", 4), c("y", "d"));

        final Table updateView2 = pool.submit(callable).get();

        TstUtils.assertTableEquals(tableStart, prevTable(updateView1));
        TstUtils.assertTableEquals(tableStart, prevTable(updateView2));

        table.notifyListeners(i(3), i(), i());

        final Table updateView3 = pool.submit(callable).get();

        TstUtils.assertTableEquals(tableStart, prevTable(updateView1));
        TstUtils.assertTableEquals(tableStart, prevTable(updateView2));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, updateView1);
        TstUtils.assertTableEquals(tableUpdate, updateView2);
        TstUtils.assertTableEquals(tableUpdate, updateView3);
    }

    public void testDropColumns() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"), c("z", 4, 8, 12));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(2, 3, 4, 6), c("x", 1, 4, 2, 3), c("y", "a", "d", "b", "c"));

        final Callable<Table> callable = () -> table.dropColumns("z");

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Table dropColumns1 = pool.submit(callable).get();

        TestCase.assertEquals(TableTools.diff(dropColumns1, tableStart, 10), "");

        TstUtils.addToTable(table, i(3), c("x", 4), c("y", "d"), c("z", 16));

        final Table dropColumns2 = pool.submit(callable).get();

        TstUtils.assertTableEquals(tableStart, prevTable(dropColumns1));
        TstUtils.assertTableEquals(tableStart, prevTable(dropColumns2));

        table.notifyListeners(i(3), i(), i());

        final Table dropColumns3 = pool.submit(callable).get();

        TstUtils.assertTableEquals(tableStart, prevTable(dropColumns1));
        TstUtils.assertTableEquals(tableStart, prevTable(dropColumns2));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, dropColumns1);
        TstUtils.assertTableEquals(tableUpdate, dropColumns2);
        TstUtils.assertTableEquals(tableUpdate, dropColumns3);
    }

    public void testWhere() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"), c("z", true, false, true));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 6), c("x", 1, 3), c("y", "a", "c"), c("z", true, true));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(2, 3, 6), c("x", 1, 4, 3), c("y", "a", "d", "c"), c("z", true, true, true));

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Table filter1 = pool.submit(() -> table.where("z")).get();

        TestCase.assertEquals(TableTools.diff(filter1, tableStart, 10), "");

        TstUtils.addToTable(table, i(3), c("x", 4), c("y", "d"), c("z", true));

        final Table filter2 = pool.submit(() -> table.where("z")).get();

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        table.notifyListeners(i(3), i(), i());

        final Table filter3 = pool.submit(() -> table.where("z")).get();

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, filter1);
        TstUtils.assertTableEquals(tableUpdate, filter2);
        TstUtils.assertTableEquals(tableUpdate, filter3);
    }

    public void testWhere2() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"), c("z", true, false, true));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 6), c("x", 1, 3), c("y", "a", "c"), c("z", true, true));
        final Table testUpdate = TstUtils.testRefreshingTable(i(3, 6), c("x", 4, 3), c("y", "d", "c"), c("z", true, true));

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Table filter1 = pool.submit(() -> table.where("z")).get();

        TestCase.assertEquals(TableTools.diff(filter1, tableStart, 10), "");

        TstUtils.addToTable(table, i(2, 3), c("x", 1, 4), c("y", "a", "d"), c("z", false, true));

        final Table filter2 = pool.submit(() -> table.where("z")).get();

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        table.notifyListeners(i(3), i(), i(2));

        final Table filter3 = pool.submit(() -> table.where("z")).get();

        TstUtils.assertTableEquals(tableStart, prevTable(filter1));
        TstUtils.assertTableEquals(tableStart, prevTable(filter2));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TableTools.showWithIndex(table);
        TableTools.showWithIndex(filter1);
        TableTools.showWithIndex(filter2);
        TableTools.showWithIndex(filter3);

        TstUtils.assertTableEquals(testUpdate, filter1);
        TstUtils.assertTableEquals(testUpdate, filter2);
        TstUtils.assertTableEquals(testUpdate, filter3);
    }

    public void testWhereDynamic() throws ExecutionException, InterruptedException {

        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"), c("z", true, false, true));
        final Table tableStart = TstUtils.testRefreshingTable(i(2, 6), c("x", 1, 3), c("y", "a", "c"), c("z", true, true));
        final Table testUpdate = TstUtils.testRefreshingTable(i(3, 6), c("x", 4, 3), c("y", "d", "c"), c("z", true, true));
        final Table whereTable = TstUtils.testRefreshingTable(i(0), c("z", true));

        final DynamicWhereFilter filter = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> new DynamicWhereFilter(whereTable, true, MatchPairFactory.getExpressions("z")));

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Future<Table> future1 = dualPool.submit(() -> table.where(filter));
        try {
            future1.get(1000, TimeUnit.MILLISECONDS);
            fail("Filtering should be blocked on LTM");
        } catch (TimeoutException ignored) {
        }
        TstUtils.addToTable(table, i(2, 3), c("x", 1, 4), c("y", "a", "d"), c("z", false, true));

        final Table filter2 = dualPool.submit(() -> table.where("z")).get();

        assertTableEquals(tableStart, prevTable(filter2));
        table.notifyListeners(i(3), i(), i(2));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        final Table filter1 = future1.get();
        TstUtils.assertTableEquals(testUpdate, filter1);
        TstUtils.assertTableEquals(filter2, filter1);
    }

    public void testSort() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"));
        final Table tableStart = TstUtils.testRefreshingTable(i(1, 2, 3), c("x", 3, 2, 1), c("y", "c", "b", "a"));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(1, 2, 3, 4), c("x", 4, 3, 2, 1), c("y", "d", "c", "b", "a"));

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Table sort1 = pool.submit(() -> table.sortDescending("x")).get();

        TestCase.assertEquals(TableTools.diff(sort1, tableStart, 10), "");

        TstUtils.addToTable(table, i(3), c("x", 4), c("y", "d"));

        final Table sort2 = pool.submit(() -> table.sortDescending("x")).get();

        TstUtils.assertTableEquals(tableStart, prevTable(sort1));
        TstUtils.assertTableEquals(tableStart, prevTable(sort2));

        table.notifyListeners(i(3), i(), i());

        final Table sort3 = pool.submit(() -> table.sortDescending("x")).get();

        TstUtils.assertTableEquals(tableStart, prevTable(sort1));
        TstUtils.assertTableEquals(tableStart, prevTable(sort2));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, sort1);
        TstUtils.assertTableEquals(tableUpdate, sort2);
        TstUtils.assertTableEquals(tableUpdate, sort3);
    }

    public void testReverse() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"));
        final Table tableStart = TstUtils.testRefreshingTable(i(1, 2, 3), c("x", 3, 2, 1), c("y", "c", "b", "a"));
        final Table tableUpdate = TstUtils.testRefreshingTable(i(1, 2, 3, 4), c("x", 4, 3, 2, 1), c("y", "d", "c", "b", "a"));
        final Table tableUpdate2 = TstUtils.testRefreshingTable(i(1, 2, 3, 4, 5), c("x", 5, 4, 3, 2, 1), c("y", "e", "d", "c", "b", "a"));
        final Table tableUpdate3 = TstUtils.testRefreshingTable(i(1, 2, 3, 4, 5, 6), c("x", 6, 5, 4, 3, 2, 1), c("y", "f", "e", "d", "c", "b", "a"));

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Table reverse1 = pool.submit(table::reverse).get();

        TestCase.assertEquals(TableTools.diff(reverse1, tableStart, 10), "");

        TstUtils.addToTable(table, i(8), c("x", 4), c("y", "d"));

        final Table reverse2 = pool.submit(table::reverse).get();

        TstUtils.assertTableEquals(tableStart, prevTable(reverse1));
        TstUtils.assertTableEquals(tableStart, prevTable(reverse2));

        table.notifyListeners(i(8), i(), i());

        final Table reverse3 = pool.submit(table::reverse).get();

        TstUtils.assertTableEquals(tableStart, prevTable(reverse1));
        TstUtils.assertTableEquals(tableStart, prevTable(reverse2));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, reverse1);
        TstUtils.assertTableEquals(tableUpdate, reverse2);
        TstUtils.assertTableEquals(tableUpdate, reverse3);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(10000), c("x", 5), c("y", "e"));
            table.notifyListeners(i(10000), i(), i());
        });
        TableTools.show(reverse1);
        TableTools.show(reverse2);
        TableTools.show(reverse3);
        assertTableEquals(tableUpdate2, reverse1);
        assertTableEquals(tableUpdate2, reverse2);
        assertTableEquals(tableUpdate2, reverse3);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(10001), c("x", 6), c("y", "f"));
            table.notifyListeners(i(10001), i(), i());
        });
        TableTools.show(reverse1);
        TableTools.show(reverse2);
        TableTools.show(reverse3);
        assertTableEquals(tableUpdate3, reverse1);
        assertTableEquals(tableUpdate3, reverse2);
        assertTableEquals(tableUpdate3, reverse3);
    }


    public void testSortOfByExternal() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "a", "a"));
        final TableMap tm = table.byExternal("y");

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        TstUtils.addToTable(table, i(3), c("x", 4), c("y", "d"));

        table.notifyListeners(i(3), i(), i());

        // We need to flush two notifications: one for the source table and one for the "withView" table in the aggregation helper.
        LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
        LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();

        final Table tableA = tm.get("a");
        final Table tableD = tm.get("d");

        TableTools.show(tableA);
        TableTools.show(tableD);

        final Table sortA = pool.submit(() -> tableA.sort("x")).get();
        final Table sortD = pool.submit(() -> tableD.sort("x")).get();

        TableTools.show(sortA);
        TableTools.show(sortD);

        TstUtils.assertTableEquals(tableD, sortD);

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
    }


    public void testChain() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"), c("z", true, false, true));
        final Table tableStart = TstUtils.testRefreshingTable(i(1, 3), c("x", 3, 1), c("y", "c", "a"), c("z", true, true), c("u", 12, 4));

        final Table tableUpdate = TstUtils.testRefreshingTable(i(1, 2, 4), c("x", 4, 3, 1), c("y", "d", "c", "a"), c("z", true, true, true), c("u", 16, 12, 4));

        final Callable<Table> callable = () -> table.updateView("u=x*4").where("z").sortDescending("x");

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Table chain1 = pool.submit(callable).get();

        TestCase.assertEquals(TableTools.diff(chain1, tableStart, 10), "");

        TstUtils.addToTable(table, i(3), c("x", 4), c("y", "d"), c("z", true));

        final Table chain2 = pool.submit(callable).get();

        TstUtils.assertTableEquals(tableStart, prevTable(chain1));
        TstUtils.assertTableEquals(tableStart, prevTable(chain2));

        table.notifyListeners(i(3), i(), i());

        final Table chain3 = pool.submit(callable).get();

        TableTools.showWithIndex(chain3);

        TstUtils.assertTableEquals(tableStart, prevTable(chain1));
        TstUtils.assertTableEquals(tableStart, prevTable(chain2));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, chain1);
        TstUtils.assertTableEquals(tableUpdate, chain2);
        TstUtils.assertTableEquals(tableUpdate, chain3);
    }

    public void testReverseLookupListener() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"));

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final ReverseLookupListener rll = pool.submit(() -> ReverseLookupListener.makeReverseLookupListenerWithSnapshot(table, "x")).get();

        TestCase.assertEquals(rll.get(1), 2);
        TestCase.assertEquals(rll.get(2), 4);
        TestCase.assertEquals(rll.get(3), 6);
        TestCase.assertEquals(rll.get(4), rll.getNoEntryValue());

        TstUtils.addToTable(table, i(4), c("x", 4), c("y", "d"));

        final ReverseLookupListener rll2 = pool.submit(() -> ReverseLookupListener.makeReverseLookupListenerWithSnapshot(table, "x")).get();

        TestCase.assertEquals(rll2.get(1), 2);
        TestCase.assertEquals(rll2.get(2), 4);
        TestCase.assertEquals(rll2.get(3), 6);
        TestCase.assertEquals(rll2.get(4), rll.getNoEntryValue());

        table.notifyListeners(i(), i(), i(4));

        final ReverseLookupListener rll3 = pool.submit(() -> ReverseLookupListener.makeReverseLookupListenerWithSnapshot(table, "x")).get();

        TestCase.assertEquals(rll3.get(1), 2);
        TestCase.assertEquals(rll3.get(2), rll.getNoEntryValue());
        TestCase.assertEquals(rll3.get(3), 6);
        TestCase.assertEquals(rll3.get(4), 4);

        TestCase.assertTrue(LogicalClock.DEFAULT.currentStep() > rll.getLastNotificationStep());
        TestCase.assertTrue(LogicalClock.DEFAULT.currentStep() > rll2.getLastNotificationStep());
        TestCase.assertEquals(LogicalClock.DEFAULT.currentStep(), rll3.getLastNotificationStep());

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TestCase.assertEquals(rll.get(1), 2);
        TestCase.assertEquals(rll.get(2), rll.getNoEntryValue());
        TestCase.assertEquals(rll.get(3), 6);
        TestCase.assertEquals(rll.get(4), 4);

        TestCase.assertEquals(rll2.get(1), 2);
        TestCase.assertEquals(rll2.get(2), rll.getNoEntryValue());
        TestCase.assertEquals(rll2.get(3), 6);
        TestCase.assertEquals(rll2.get(4), 4);

        TestCase.assertEquals(rll3.get(1), 2);
        TestCase.assertEquals(rll3.get(2), rll.getNoEntryValue());
        TestCase.assertEquals(rll3.get(3), 6);
        TestCase.assertEquals(rll3.get(4), 4);

        TestCase.assertEquals(LogicalClock.DEFAULT.currentStep(), rll.getLastNotificationStep());
        TestCase.assertEquals(LogicalClock.DEFAULT.currentStep(), rll2.getLastNotificationStep());
        TestCase.assertEquals(LogicalClock.DEFAULT.currentStep(), rll3.getLastNotificationStep());
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
        transformations.add(t -> t.where(DisjunctiveFilter.makeDisjunctiveFilter(SelectFilterFactory.expandQuickFilter(t, "10", QuickFilterMode.NORMAL))));
        transformations.add(t -> t.sortDescending("doubleCol"));
        transformations.add(Table::flatten);
        testIterative(transformations);
    }

    public void testIterativeDisjunctiveCondition() {
        final List<Function<Table, Table>> transformations = new ArrayList<>();
        transformations.add(t -> t.where(DisjunctiveFilter.makeDisjunctiveFilter(ConditionFilter.createConditionFilter("false"), ConditionFilter.createConditionFilter("true"))));
        testIterative(transformations);
    }

    private void testIterative(List<Function<Table, Table>> transformations) {
        testIterative(transformations, 0, new MutableInt(50));
    }

    private void testIterative(List<Function<Table, Table>> transformations, int seed, MutableInt numSteps) {
        final TstUtils.ColumnInfo[] columnInfos;

        final int size = 100;
        final Random random = new Random(seed);
        final int maxSteps = numSteps.intValue();

        final QueryTable table = getTable(size, random, columnInfos = initColumnInfos(new String[]{"Sym", "intCol", "boolCol", "boolCol2", "doubleCol"},
                new SetGenerator<>("aa", "bb", "bc", "cc", "dd", "ee", "ff", "gg", "hh", "ii"),
                new IntGenerator(0, 100),
                new BooleanGenerator(),
                new BooleanGenerator(),
                new DoubleGenerator(0, 100)
        ));


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

        final Table standard = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> {
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
        //noinspection MismatchedQueryAndUpdateOfCollection
        final List<ShiftAwareListener> listeners = new ArrayList<>();
        int lastResultSize = 0;

        try {
            if (LiveTableTestCase.printTableUpdates) {
                System.out.println("Input Table:\n");
                TableTools.showWithIndex(table);
            }

            for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
                final int i = numSteps.intValue();
                if (LiveTableTestCase.printTableUpdates) {
                    System.out.println("Step = " + i);
                }

                final List<Table> beforeStartFirstHalf = new ArrayList<>(splitCallables.size());
                for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                    beforeStartFirstHalf.add(pool.submit(splitCallable.first).get());
                }

                LiveTableMonitor.DEFAULT.startCycleForUnitTests();

                if (beforeUpdate) {
                    // before we update the underlying data
                    final Table chain1 = pool.submit(complete).get();
                    chain1.setAttribute("Step", i);
                    chain1.setAttribute("Type", "beforeUpdate");
                    results.add(chain1);
                }

                if (beforeStartAndBeforeUpdate) {
                    final List<Table> beforeStartAndBeforeUpdateSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second.apply(beforeStartFirstHalf.get(fSplitIndex))).get();
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeUpdateSplit");
                        beforeStartAndBeforeUpdateSplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndBeforeUpdateSplitResults);
                }

                final List<Table> beforeUpdateFirstHalf = new ArrayList<>(splitCallables.size());
                for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                    beforeUpdateFirstHalf.add(pool.submit(splitCallable.first).get());
                }

                final Index[] updates = GenerateTableUpdates.computeTableUpdates(size, random, table, columnInfos);

                if (beforeNotify) {
                    // after we update the underlying data, but before we notify
                    final Table chain2 = pool.submit(complete).get();
                    chain2.setAttribute("Step", i);
                    chain2.setAttribute("Type", "beforeNotify");
                    results.add(chain2);
                }

                if (beforeAndAfterUpdate) {
                    final List<Table> beforeAndAfterUpdateSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second.apply(beforeUpdateFirstHalf.get(fSplitIndex))).get();
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeAndAfterUpdateSplit");
                        splitResult.setAttribute("SplitIndex", splitIndex);
                        beforeAndAfterUpdateSplitResults.add(splitResult);
                    }
                    results.addAll(beforeAndAfterUpdateSplitResults);
                }

                if (beforeStartAndAfterUpdate) {
                    final List<Table> beforeStartAndAfterUpdateSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second.apply(beforeStartFirstHalf.get(fSplitIndex))).get();
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeStartAndAfterUpdate");
                        splitResult.setAttribute("SplitIndex", splitIndex);
                        beforeStartAndAfterUpdateSplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndAfterUpdateSplitResults);
                }

                final List<Table> beforeNotifyFirstHalf = new ArrayList<>(splitCallables.size());
                for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                    beforeNotifyFirstHalf.add(pool.submit(splitCallable.first).get());
                }

                table.notifyListeners(updates[0], updates[1], updates[2]);

                if (beforeAndAfterNotify) {
                    final List<Table> beforeAndAfterNotifySplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second.apply(beforeNotifyFirstHalf.get(fSplitIndex))).get();
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeAndAfterNotify");
                        beforeAndAfterNotifySplitResults.add(splitResult);
                    }
                    results.addAll(beforeAndAfterNotifySplitResults);
                }

                if (beforeStartAndAfterNotify) {
                    final List<Table> beforeStartAndAfterNotifySplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second.apply(beforeStartFirstHalf.get(fSplitIndex))).get();
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeStartAndAfterNotify");
                        splitResult.setAttribute("SplitIndex", splitIndex);
                        beforeStartAndAfterNotifySplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndAfterNotifySplitResults);
                }

                if (beforeUpdateAndAfterNotify) {
                    final List<Table> beforeUpdateAndAfterNotifySplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second.apply(beforeUpdateFirstHalf.get(fSplitIndex))).get();
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeUpdateAndAfterNotify");
                        splitResult.setAttribute("SplitIndex", splitIndex);
                        beforeUpdateAndAfterNotifySplitResults.add(splitResult);
                    }
                    results.addAll(beforeUpdateAndAfterNotifySplitResults);
                }

                final List<Table> beforeCycleFirstHalf = new ArrayList<>(splitCallables.size());
                if (beforeAndAfterCycle) {
                    for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                        beforeCycleFirstHalf.add(pool.submit(splitCallable.first).get());
                    }
                }

                if (beforeNotify) {
                    // after notification, on the same cycle
                    final Table chain3 = pool.submit(complete).get();
                    chain3.setAttribute("Step", i);
                    chain3.setAttribute("Type", "beforeNotify");
                    results.add(chain3);
                }

                for (int newResult = lastResultSize; newResult < results.size(); ++newResult) {
                    final DynamicTable dynamicTable = (DynamicTable) results.get(newResult);
                    final InstrumentedShiftAwareListenerAdapter listener = new InstrumentedShiftAwareListenerAdapter("errorListener", dynamicTable, false) {
                        @Override
                        public void onUpdate(final Update upstream) {}

                        @Override
                        public void onFailureInternal(Throwable originalException, UpdatePerformanceTracker.Entry sourceEntry) {
                            originalException.printStackTrace(System.err);
                            TestCase.fail(originalException.getMessage());
                        }
                    };
                    listeners.add(listener);
                    dynamicTable.listenForUpdates(listener);
                }
                lastResultSize = results.size();
                LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

                if (beforeStartAndAfterCycle) {
                    final List<Table> beforeStartAndAfterCycleSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second.apply(beforeStartFirstHalf.get(fSplitIndex))).get();
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeStartAndAfterCycle");
                        splitResult.setAttribute("SplitIndex", splitIndex);
                        beforeStartAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> splitCallables.get(fSplitIndex).second.apply(beforeStartFirstHalf.get(fSplitIndex)));
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeStartAndAfterCycleLocked");
                        splitResult.setAttribute("SplitIndex", splitIndex);
                        beforeStartAndAfterCycleSplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndAfterCycleSplitResults);
                }

                if (beforeUpdateAndAfterCycle) {
                    final List<Table> beforeUpdateAndAfterCycleSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second.apply(beforeUpdateFirstHalf.get(fSplitIndex))).get();
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeUpdateAndAfterCycle");
                        splitResult.setAttribute("SplitIndex", splitIndex);
                        beforeUpdateAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> splitCallables.get(fSplitIndex).second.apply(beforeUpdateFirstHalf.get(fSplitIndex)));
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeUpdateAndAfterCycleLocked");
                        splitResult.setAttribute("SplitIndex", splitIndex);
                        beforeUpdateAndAfterCycleSplitResults.add(splitResult);
                    }
                    results.addAll(beforeUpdateAndAfterCycleSplitResults);
                }

                if (beforeNotifyAndAfterCycle) {
                    final List<Table> beforeNotifyAndAfterCycleSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second.apply(beforeNotifyFirstHalf.get(fSplitIndex))).get();
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeNotifyAndAfterCycle");
                        splitResult.setAttribute("SplitIndex", splitIndex);
                        beforeNotifyAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final Table splitResult = splitCallables.get(splitIndex).second.apply(beforeNotifyFirstHalf.get(splitIndex));
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeNotifyAndAfterCycleLocked");
                        splitResult.setAttribute("SplitIndex", splitIndex);
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
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second.apply(beforeCycleFirstHalf.get(fSplitIndex))).get();
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeAndAfterCycle");
                        splitResult.setAttribute("SplitIndex", splitIndex);
                        beforeAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> splitCallables.get(fSplitIndex).second.apply(beforeCycleFirstHalf.get(fSplitIndex)));
                        splitResult.setAttribute("Step", i);
                        splitResult.setAttribute("Type", "beforeAndAfterCycle");
                        splitResult.setAttribute("SplitIndex", splitIndex);
                        beforeAndAfterCycleSplitResults.add(splitResult);
                    }

                    results.addAll(beforeAndAfterCycleSplitResults);
                }

                if (LiveTableTestCase.printTableUpdates) {
                    System.out.println("Input Table: (" + Objects.hashCode(table) + ")");
                    TableTools.showWithIndex(table);
                    System.out.println("Standard Table: (" + Objects.hashCode(standard) + ")");
                    TableTools.showWithIndex(standard);
                    System.out.println("Verifying " + results.size() + " tables (size = " + standard.size() +")");
                }

                // now verify all the outstanding results
                for (Table checkTable : results) {
                    String diff = diff(checkTable, standard, 10);
                    if (!diff.isEmpty() && LiveTableTestCase.printTableUpdates) {
                        System.out.println("Check Table: " + checkTable.getAttribute("Step") + ", " +
                                checkTable.getAttribute("Type") +
                                ", splitIndex=" + checkTable.getAttribute("SplitIndex") +
                                ", hash=" + Objects.hashCode(checkTable));
                        TableTools.showWithIndex(checkTable);
                    }
                    TestCase.assertEquals("", diff);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            TestCase.fail(e.getMessage());
        }
    }

    public void testSelectDistinct() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6, 8), c("y", "a", "b", "a", "c"));
        final Table expected1 = newTable(c("y", "a", "b", "c"));
        final Table expected2 = newTable(c("y", "a", "d", "b", "c"));
        final Table expected2outOfOrder = newTable(c("y", "a", "b", "c", "d"));

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Callable<Table> callable = () -> table.selectDistinct("y");

        final Table distinct1 = pool.submit(callable).get();

        TstUtils.assertTableEquals(expected1, distinct1);

        TstUtils.addToTable(table, i(3), c("y", "d"));

        TstUtils.assertTableEquals(expected1, distinct1);

        final Table distinct2 = pool.submit(callable).get();

        TstUtils.assertTableEquals(expected1, distinct2);
        TstUtils.assertTableEquals(expected1, prevTable(distinct2));

        table.notifyListeners(i(3), i(), i());

        final Table distinct3 = pool.submit(callable).get();

        TstUtils.assertTableEquals(expected1, prevTable(distinct1));
        TstUtils.assertTableEquals(expected1, prevTable(distinct2));
        TstUtils.assertTableEquals(expected2, distinct3);
        TstUtils.assertTableEquals(expected2, prevTable(distinct3));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TstUtils.assertTableEquals(expected2outOfOrder, distinct1);
        TstUtils.assertTableEquals(expected2outOfOrder, distinct2);
        TstUtils.assertTableEquals(expected2, distinct3);
        TstUtils.assertTableEquals(expected2, prevTable(distinct3));
    }

    @ReflexiveUse(referrers = "io.deephaven.db.v2.TestConcurrentInstantiation")
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
                    if (now < endTime) {
                        invocationCount.wait(endTime - now);
                    }
                    now = System.currentTimeMillis();
                }
                if (invocationCount.get() < count) {
                    throw new RuntimeException("Invocation count did not advance.");
                }
            }
        }
    }

    public void testSelectDistinctReset() throws ExecutionException, InterruptedException {
        final BarrierFunction barrierFunction = new BarrierFunction();
        QueryScope.addParam("barrierFunction", barrierFunction);

        try {
            final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6, 8), c("y", "a", "b", "a", "c"));
            final Table slowed = table.updateView("z=barrierFunction.apply(y)");
            final Table expected1 = newTable(c("z", "a", "b"));

            LiveTableMonitor.DEFAULT.startCycleForUnitTests();

            final Callable<Table> callable = () -> slowed.selectDistinct("z");

            final Future<Table> future1 = pool.submit(callable);
            barrierFunction.waitForInvocation(2, 5000);

            System.out.println("Removing rows");
            removeRows(table, i(8));
            table.notifyListeners(i(), i(8), i());

            barrierFunction.sleepDuration = 0;

            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

            final Table distinct1 = future1.get();
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

    public void testMinMaxBy() throws Exception {
        testByConcurrent(t -> t.maxBy("KeyColumn"));
        testByConcurrent(t -> t.minBy("KeyColumn"));
        testByConcurrent(t -> t.by(new AddOnlyMinMaxByStateFactoryImpl(true), "KeyColumn"), true, false, false, true);
        testByConcurrent(t -> t.by(new AddOnlyMinMaxByStateFactoryImpl(false), "KeyColumn"), true, false, false, true);
    }

    public void testFirstLastBy() throws Exception {
        testByConcurrent(t -> t.firstBy("KeyColumn"));
        testByConcurrent(t -> t.lastBy("KeyColumn"));
        testByConcurrent(t -> t.by(new TrackingFirstByStateFactoryImpl(), "KeyColumn"));
        testByConcurrent(t -> t.by(new TrackingLastByStateFactoryImpl(), "KeyColumn"));
    }

    public void testSortedFirstLastBy() throws Exception {
        testByConcurrent(t -> t.by(new SortedFirstBy("IntCol"), "KeyColumn"));
        testByConcurrent(t -> t.by(new SortedLastBy("IntCol"), "KeyColumn"));
    }

    public void testKeyedBy() throws Exception {
        testByConcurrent(t -> t.by("KeyColumn"));
    }

    public void testNoKeyBy() throws Exception {
        testByConcurrent(Table::by, false, false, true, true);
    }

    public void testPercentileBy() throws Exception {
        testByConcurrent(t -> t.dropColumns("KeyColumn").by(new PercentileByStateFactoryImpl(0.25)), false, false, true, true);
        testByConcurrent(t -> t.dropColumns("KeyColumn").by(new PercentileByStateFactoryImpl(0.75)), false, false, true, true);
        testByConcurrent(t -> t.medianBy("KeyColumn"));
    }

    public void testAggCombo() throws Exception {
        testByConcurrent(t -> t.by(AggCombo(AggAvg("AvgInt=IntCol"), AggCount("NumInts"), AggSum("SumDouble=DoubleCol"), AggMax("MaxDouble=DoubleCol")), "KeyColumn"));
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

    private void testByConcurrent(Function<Table, Table> function, boolean hasKeys, boolean withReset, boolean allowModifications, boolean haveBigNumerics) throws Exception {
        setExpectError(false);

        final QueryTable table = makeByConcurrentBaseTable(haveBigNumerics);
        final QueryTable table2 = makeByConcurrentStep2Table(allowModifications, haveBigNumerics);

        final BarrierFunction barrierFunction =  withReset ? new BarrierFunction() : null;
        QueryScope.addParam("barrierFunction", barrierFunction);

        try {
            final Callable<Table> callable;
            final Table slowed;
            if (withReset) {
                QueryLibrary.importStatic(TestConcurrentInstantiation.class);

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

            // We only care about the silent version of this table, as it's just a vessel to tick and ensure that the resultant table
            // is computed using the appropriate version.
            final Table expected1 = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> function.apply(table.silent()).select());
            final Table expected2 = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> function.apply(table2));

            LiveTableMonitor.DEFAULT.startCycleForUnitTests();

            final Future<Table> future1 = pool.submit(callable);
            final Table result1;
            if (withReset) {
                barrierFunction.waitForInvocation(2, 5000);
            }
            result1 = future1.get();

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

            final Table result2 = pool.submit(callable).get();

            System.out.println("Result 2");
            TableTools.show(result2);
            System.out.println("Expected 1");
            TableTools.show(expected1);

            // The column sources are redirected, and the underlying table has been updated without a notification _yet_,
            // so the column sources have _already_ changed and we are inside an update cycle, so the value of get() is indeterminate
            // therefore this assert is not really a valid thing to do.
            //TstUtils.assertTableEquals(expected1, result2);
            final Table prevResult2a = prevTable(result2);
            System.out.println("Prev Result 2a");
            TableTools.show(prevResult2a);

            TstUtils.assertTableEquals(expected1, prevResult2a, TableDiff.DiffItems.DoublesExact);

            table.notifyListeners(i(5, 9), i(), allowModifications ? i(8) : i());

            final Future<Table> future3 = pool.submit(callable);
            if (withReset) {
                while (((QueryTable) slowed).getLastNotificationStep() != LogicalClock.DEFAULT.currentStep()) {
                    LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
                }
            }
            final Table result3 = future3.get();

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

            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

            if (hasKeys) {
                TstUtils.assertTableEquals(expected2.sort("KeyColumn"), result1.sort("KeyColumn"), TableDiff.DiffItems.DoublesExact);
                TstUtils.assertTableEquals(expected2.sort("KeyColumn"), result2.sort("KeyColumn"), TableDiff.DiffItems.DoublesExact);
            } else {
                TstUtils.assertTableEquals(expected2, result1, TableDiff.DiffItems.DoublesExact);
                TstUtils.assertTableEquals(expected2, result2, TableDiff.DiffItems.DoublesExact);
            }
            TstUtils.assertTableEquals(expected2, result3, TableDiff.DiffItems.DoublesExact);

        } finally {
            QueryScope.addParam("barrierFunction", null);
        }
    }

    public void testByExternalConcurrent() throws Exception {
        testByExternalConcurrent(false);
        testByExternalConcurrent(true);
    }

    private void testByExternalConcurrent(boolean withReset) throws Exception {
        setExpectError(false);

        final QueryTable table = makeByConcurrentBaseTable(false);
        final QueryTable table2 = makeByConcurrentStep2Table(true, false);


        final Callable<TableMap> callable;
        final Table slowed;
        if (withReset) {
            QueryLibrary.importStatic(TestConcurrentInstantiation.class);

            slowed = table.updateView("KeyColumn=identitySleep(KeyColumn)");
            callable = () -> slowed.byExternal("KeyColumn");
        } else {
            slowed = null;
            callable = () -> table.byExternal("KeyColumn");
        }

        // We only care about the silent version of this table, as it's just a vessel to tick and ensure that the resultant table
        // is computed using the appropriate version.
        final Table expected1 = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> table.silent().byExternal("KeyColumn").merge().select());
        final Table expected2 = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> table2.silent().byExternal("KeyColumn").merge().select());

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Future<TableMap> future1 = pool.submit(callable);
        final TableMap result1;
        if (withReset) {
            SleepUtil.sleep(25);
        }
        result1 = future1.get();

        System.out.println("Result 1");
        final Table result1a = result1.get("a");
        final Table result1b = result1.get("b");
        final Table result1c = result1.get("c");
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

        final TableMap result2 = pool.submit(callable).get();

        System.out.println("Result 2");
        final Table result2a = result2.get("a");
        final Table result2b = result2.get("b");
        final Table result2c = result2.get("c");
        final Table result2d_1 = result2.get("d");
        assertNull(result2d_1);

        TableTools.show(result2a);
        TableTools.show(result2b);
        TableTools.show(result2c);
        System.out.println("Expected 1");
        TableTools.show(expected1);

        table.notifyListeners(i(5, 9), i(), i(8));

        final Future<TableMap> future3 = pool.submit(callable);
        if (withReset) {
            while (((QueryTable) slowed).getLastNotificationStep() != LogicalClock.DEFAULT.currentStep()) {
                LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
            }
        }
        final TableMap result3 = future3.get();

        System.out.println("Result 3");
        final Table result3a = result3.get("a");
        final Table result3b = result3.get("b");
        final Table result3c = result3.get("c");
        final Table result3d = result3.get("d");

        System.out.println("Expected 2");
        TableTools.show(expected2);

        TstUtils.assertTableEquals(expected2.where("KeyColumn = `a`"), result3a);
        TstUtils.assertTableEquals(expected2.where("KeyColumn = `b`"), result3b);
        assertNull(result3c);
        TstUtils.assertTableEquals(expected2.where("KeyColumn = `d`"), result3d);

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TstUtils.assertTableEquals(expected2, result1.merge());
        TstUtils.assertTableEquals(expected2, result2.merge());
        TstUtils.assertTableEquals(expected2, result3.merge());
    }

    private QueryTable makeByConcurrentBaseTable(boolean haveBigNumerics) {
        final List<ColumnHolder> columnHolders = new ArrayList<>(Arrays.asList(
        c("KeyColumn", "a", "b", "a", "c"),
                intCol("IntCol", 1, 2, 3, 4),
                doubleCol("DoubleCol", 100.1, 200.2, 300.3, 400.4),
                floatCol("FloatCol", 10.1f, 20.2f, 30.3f, 40.4f),
                shortCol("ShortCol", (short)10, (short)20, (short)30, (short)40),
                byteCol("ByteCol", (byte)11, (byte)12, (byte)13, (byte)14),
                charCol("CharCol", 'A', 'B', 'C', 'D'),
                longCol("LongCol", 10_000_000_000L, 20_000_000_000L, 30_000_000_000L, 40_000_000_000L)));

        if (haveBigNumerics) {
            columnHolders.add(col("BigDecCol", BigDecimal.valueOf(10000.1), BigDecimal.valueOf(20000.2), BigDecimal.valueOf(40000.3), BigDecimal.valueOf(40000.4)));
            columnHolders.add(col("BigIntCol", BigInteger.valueOf(100000), BigInteger.valueOf(200000), BigInteger.valueOf(300000), BigInteger.valueOf(400000)));
        }


        return TstUtils.testRefreshingTable(i(2, 4, 6, 8), columnHolders.toArray(ColumnHolder.ZERO_LENGTH_COLUMN_HOLDER_ARRAY));
    }

    private QueryTable makeByConcurrentStep2Table(boolean allowModifications, boolean haveBigNumerics) {
        final QueryTable table2 = makeByConcurrentBaseTable(haveBigNumerics);
        doByConcurrentAdditions(table2, haveBigNumerics);
        if(allowModifications) {
            doByConcurrentModifications(table2, haveBigNumerics);
        }
        return table2;

    }

    private void doByConcurrentModifications(QueryTable table, boolean haveBigNumerics) {
        final List<ColumnHolder> columnHolders = new ArrayList<>(Arrays.asList(
                c("KeyColumn", "b"),
                intCol("IntCol", 7),
                doubleCol("DoubleCol", 700.7),
                floatCol("FloatCol", 70.7f),
                shortCol("ShortCol", (short)70),
                byteCol("ByteCol", (byte)17),
                charCol("CharCol", 'E'),
                longCol("LongCol", 70_000_000_000L)));
        if (haveBigNumerics) {
            columnHolders.addAll(Arrays.asList(
                    col("BigDecCol", BigDecimal.valueOf(70000.7)),
                    col("BigIntCol", BigInteger.valueOf(700000))
            ));
        }

        TstUtils.addToTable(table, i(8), columnHolders.toArray(ColumnHolder.ZERO_LENGTH_COLUMN_HOLDER_ARRAY));
    }

    private void doByConcurrentAdditions(QueryTable table, boolean haveBigNumerics) {

        final List<ColumnHolder> columnHolders = new ArrayList<>(Arrays.asList(
                c("KeyColumn", "d", "a"),
                intCol("IntCol", 5, 6),
                doubleCol("DoubleCol", 505.5, 600.6),
                floatCol("FloatCol", 50.5f, 60.6f),
                shortCol("ShortCol", (short)50, (short)60),
                byteCol("ByteCol", (byte)15, (byte)16),
                charCol("CharCol", 'E', 'F'),
                longCol("LongCol", 50_000_000_000L, 60_000_000_000L)));

        if (haveBigNumerics) {
            columnHolders.addAll(Arrays.asList(
                    col("BigDecCol", BigDecimal.valueOf(50000.5), BigDecimal.valueOf(60000.6)),
                    col("BigIntCol", BigInteger.valueOf(500000), BigInteger.valueOf(600000))
            ));
        }

        TstUtils.addToTable(table, i(5, 9), columnHolders.toArray(ColumnHolder.ZERO_LENGTH_COLUMN_HOLDER_ARRAY));
    }

    public void testConstructSnapshotException() throws ExecutionException, InterruptedException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6, 8), c("y", "a", "b", "c", "d"));


        final Future<String[]> future = pool.submit(() -> {
            final MutableObject<String[]> result = new MutableObject<>();
            ConstructSnapshot.callDataSnapshotFunction("testConstructSnapshotException", ConstructSnapshot.makeSnapshotControl(false, table), (usePrev, clock) -> {
                Assert.eqFalse(usePrev, "usePrev");
                final int size = table.intSize();
                final String [] result1 = new String[size];
                result.setValue(result1);
                // on the first pass, we want to have an AAIOBE for the result1, which will occur, because 100ms
                // into this sleep; the index size will increase by 1
                SleepUtil.sleep(1000);

                // and make sure the terrible thing has happened
                if (result1.length == 4) {
                    Assert.eq(table.getIndex().size(), "table.getIndex().size()", 5);
                }

                //noinspection unchecked
                final ColumnSource<String> cs = table.getColumnSource("y");

                int ii = 0;
                for (final Index.Iterator it = table.getIndex().iterator(); it.hasNext(); ) {
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
        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        TstUtils.addToTable(table, i(10), c("y","e"));
        table.notifyListeners(i(10), i(), i());
        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        // now get the answer
        final String [] answer = future.get();

        assertEquals(Arrays.asList("a", "b", "c", "d", "e"),  Arrays.asList(answer));
    }

    public void testStaticSnapshot() throws ExecutionException, InterruptedException {
        final Table emptyTable = TableTools.emptyTable(0);

        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), c("x", 1, 2, 3), c("y", "a", "b", "c"), c("z", true, false, true));
        final Table tableStart = TableTools.newTable(c("x", 1, 2, 3), c("y", "a", "b", "c"), c("z", true, false, true));
        final Table tableUpdate = TableTools.newTable(c("x", 1, 4, 2, 3), c("y", "a", "d", "b", "c"), c("z", true, true, false, true));

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();

        final Table snap1 = pool.submit(() -> emptyTable.snapshot(table)).get();

        TestCase.assertEquals(TableTools.diff(snap1, tableStart, 10), "");

        TstUtils.addToTable(table, i(3), c("x", 4), c("y", "d"), c("z", true));

        final Table snap2 = pool.submit(() -> emptyTable.snapshot(table)).get();

        TstUtils.assertTableEquals(tableStart, prevTable(snap1));
        TstUtils.assertTableEquals(tableStart, prevTable(snap2));

        table.notifyListeners(i(3), i(), i());

        final Table snap3 = pool.submit(() -> emptyTable.snapshot(table)).get();

        TstUtils.assertTableEquals(tableStart, prevTable(snap1));
        TstUtils.assertTableEquals(tableStart, prevTable(snap2));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableStart, snap1);
        TstUtils.assertTableEquals(tableStart, snap2);
        TstUtils.assertTableEquals(tableUpdate, snap3);
    }

    public void testSnapshotLiveness() {
        final QueryTable left, right, snap;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            right = TstUtils.testRefreshingTable(i(0), c("x", 1));
            left = TstUtils.testRefreshingTable(i());
            snap = (QueryTable) left.snapshot(right);
            snap.retainReference();
        }

        // assert each table is still alive w.r.t. Liveness
        for (final QueryTable t : new QueryTable[] {left, right, snap}) {
            t.retainReference();
            t.dropReference();
        }

        TstUtils.assertTableEquals(snap, right);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update(i(1), i(), i(),
                    IndexShiftData.EMPTY, ModifiedColumnSet.EMPTY);
            TstUtils.addToTable(right, downstream.added, c("x", 2));
            right.notifyListeners(downstream);
        });
        TstUtils.assertTableEquals(snap, prevTable(right));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update(i(1), i(), i(),
                    IndexShiftData.EMPTY, ModifiedColumnSet.EMPTY);
            TstUtils.addToTable(left, downstream.added);
            left.notifyListeners(downstream);
        });
        TstUtils.assertTableEquals(snap, right);
    }
}
