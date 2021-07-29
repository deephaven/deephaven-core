/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;

import java.util.Random;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.v2.TstUtils.*;

@Category(OutOfBandTest.class)
public class TestSyncTableFilter extends LiveTableTestCase {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setExpectError(false);
    }

    public void testSimple() {
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, 2, 2, 3, 3), intCol("Sentinel", 101, 102, 103, 104, 105, 106), col("Key", "a", "a", "a", "a", "a", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 0, 0, 2, 2, 4, 4), intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Key", "a", "a", "a", "a", "a", "a"));

        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder("ID");
        builder.addTable("a", a);
        builder.addTable("b", b);
        final TableMap result = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(builder::build);

        assertEquals(new String[]{"a", "b"}, result.getKeySet());

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        TableTools.show(fa);
        TableTools.show(fb);

        final DynamicTable ex1a = newTable(longCol("ID", 2, 2), intCol("Sentinel", 103, 104), col("Key", "a", "a"));
        final DynamicTable ex1b = newTable(longCol("ID", 2, 2), intCol("Sentinel", 203, 204), col("Key", "a", "a"));

        assertEquals("", TableTools.diff(fa, ex1a, 10));
        assertEquals("", TableTools.diff(fb, ex1b, 10));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
            a.notifyListeners(i(10, 11), i(), i());
        });

        assertEquals("", TableTools.diff(fa, ex1a, 10));
        assertEquals("", TableTools.diff(fb, ex1b, 10));

        final DynamicTable ex2a = newTable(longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
        final DynamicTable ex2b = newTable(longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));
            b.notifyListeners(i(10, 11), i(), i());
        });

        TableTools.showWithIndex(fa);
        TableTools.showWithIndex(fb);

        assertEquals("", TableTools.diff(fa, ex2a, 10));
        assertEquals("", TableTools.diff(fb, ex2b, 10));
    }

    public void testSimpleAddAgain() {
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, 2, 2, 3, 3), intCol("Sentinel", 101, 102, 103, 104, 105, 106), col("Key", "a", "a", "a", "a", "a", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 0, 0, 2, 2, 4, 4), intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Key", "a", "a", "a", "a", "a", "a"));

        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder("ID");
        builder.addTable("a", a);
        builder.addTable("b", b);
        final TableMap result = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(builder::build);

        assertEquals(new String[]{"a", "b"}, result.getKeySet());

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        TableTools.show(fa);
        TableTools.show(fb);

        final DynamicTable ex1a = newTable(longCol("ID", 2, 2), intCol("Sentinel", 103, 104), col("Key", "a", "a"));
        final DynamicTable ex1b = newTable(longCol("ID", 2, 2), intCol("Sentinel", 203, 204), col("Key", "a", "a"));

        assertEquals("", TableTools.diff(fa, ex1a, 10));
        assertEquals("", TableTools.diff(fb, ex1b, 10));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
            a.notifyListeners(i(10, 11), i(), i());
        });

        assertEquals("", TableTools.diff(fa, ex1a, 10));
        assertEquals("", TableTools.diff(fb, ex1b, 10));

        final DynamicTable ex2a = newTable(longCol("ID", 5, 5, 5, 5), intCol("Sentinel", 107, 108, 109, 110), col("Key", "b", "b", "c", "c"));
        final DynamicTable ex2b = newTable(longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));
            b.notifyListeners(i(10, 11), i(), i());
            TstUtils.addToTable(a, i(12, 13), longCol("ID", 5, 5), intCol("Sentinel", 109, 110), col("Key", "c", "c"));
            a.notifyListeners(i(12, 13), i(), i());
        });

        TableTools.showWithIndex(fa);
        TableTools.showWithIndex(fb);

        assertEquals("", TableTools.diff(fa, ex2a, 10));
        assertEquals("", TableTools.diff(fb, ex2b, 10));

        final DynamicTable ex3b = newTable(longCol("ID", 5, 5, 5), intCol("Sentinel", 207, 208, 209), col("Key", "a", "a", "a"));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(12, 13), longCol("ID", 5, 6), intCol("Sentinel", 209, 210), col("Key", "a", "a"));
            b.notifyListeners(i(12, 13), i(), i());
        });

        TableTools.showWithIndex(fa);
        TableTools.showWithIndex(fb);

        assertEquals("", TableTools.diff(fa, ex2a, 10));
        assertEquals("", TableTools.diff(fb, ex3b, 10));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(14, 15), longCol("ID", 5, 6), intCol("Sentinel", 111, 112), col("Key", "a", "a"));
            a.notifyListeners(i(14, 15), i(), i());
        });

        System.out.println("A advanced to 6");
        TableTools.showWithIndex(fa);
        TableTools.showWithIndex(fb);

        final DynamicTable ex4a = newTable(longCol("ID", 6), intCol("Sentinel", 112), col("Key", "a"));
        final DynamicTable ex4b = newTable(longCol("ID", 6), intCol("Sentinel", 210), col("Key", "a"));

        assertEquals("", TableTools.diff(fa, ex4a, 10));
        assertEquals("", TableTools.diff(fb, ex4b, 10));
    }

    public void testNullAppearance() {
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, QueryConstants.NULL_LONG, QueryConstants.NULL_LONG, 3, 3), intCol("Sentinel", 101, 102, 103, 104, 105, 106), col("Key", "a", "a", "a", "a", "a", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("ID", 0, 0, 2, 2, 4, 4), intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Key", "a", "a", "a", "a", "a", "a"));

        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder().defaultId("ID").defaultKeys()
                .addTable("a", a)
                .addTable("b", b, "ID");
        final TableMap result = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(builder::build);

        assertEquals(new String[]{"a", "b"}, result.getKeySet());

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        TableTools.show(fa);
        TableTools.show(fb);

        final Table empty = a.getSubTable(i());

        assertEquals("", TableTools.diff(fa, empty, 10));
        assertEquals("", TableTools.diff(fb, empty, 10));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
            TstUtils.addToTable(a, i(2, 3), longCol("ID", 2, 2), intCol("Sentinel", 103, 104), col("Key", "a", "a"));
            a.notifyListeners(i(10, 11), i(), i(2, 3));
        });

        final DynamicTable ex1a = newTable(longCol("ID", 2, 2), intCol("Sentinel", 103, 104), col("Key", "a", "a"));
        final DynamicTable ex1b = newTable(longCol("ID", 2, 2), intCol("Sentinel", 203, 204), col("Key", "a", "a"));

        assertEquals("", TableTools.diff(fa, ex1a, 10));
        assertEquals("", TableTools.diff(fb, ex1b, 10));

        final DynamicTable ex2a = newTable(longCol("ID", 5, 5), intCol("Sentinel", 107, 108), col("Key", "b", "b"));
        final DynamicTable ex2b = newTable(longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10, 11), longCol("ID", 5, 5), intCol("Sentinel", 207, 208), col("Key", "a", "a"));
            b.notifyListeners(i(10, 11), i(), i());
        });

        TableTools.showWithIndex(fa);
        TableTools.showWithIndex(fb);

        assertEquals("", TableTools.diff(fa, ex2a, 10));
        assertEquals("", TableTools.diff(fb, ex2b, 10));
    }

    public void testSimpleKeyed() {
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, 2, 2, 3, 3, 4, 4, 5, 5), intCol("Sentinel", 101, 102, 103, 104, 105, 106, 107, 108, 109, 110), col("Key", "a", "a", "b", "b", "a", "a", "a", "a", "b", "b"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("Ego", 0, 0, 2, 2, 4, 4), intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Klyuch", "a", "a", "b", "b", "a", "a"));

        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder();
        builder.addTable("a", a, "ID", "Key");
        builder.addTable("b", b, "Ego", "Klyuch");
        final TableMap result = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(builder::build);

        assertEquals(new String[]{"a", "b"}, result.getKeySet());

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        final TableUpdateValidator tuvfa = TableUpdateValidator.make("fa", (QueryTable) fa);
        final FailureListener fla = new FailureListener();
        tuvfa.getResultTable().listenForUpdates(fla);
        final TableUpdateValidator tuvfb = TableUpdateValidator.make("fa", (QueryTable) fb);
        final FailureListener flb = new FailureListener();
        tuvfb.getResultTable().listenForUpdates(flb);

        TableTools.show(fa);
        TableTools.show(fb);

        final DynamicTable ex1a = newTable(longCol("ID", 2, 2, 4, 4), intCol("Sentinel", 103, 104, 107, 108), col("Key", "b", "b", "a", "a"));
        final DynamicTable ex1b = newTable(longCol("Ego", 2, 2, 4, 4), intCol("Sentinel", 203, 204, 205, 206), col("Klyuch", "b", "b", "a", "a"));

        assertEquals("", TableTools.diff(fa, ex1a, 10));
        assertEquals("", TableTools.diff(fb, ex1b, 10));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(b, i(10, 11), longCol("Ego", 5, 5), intCol("Sentinel", 207, 208), col("Klyuch", "b", "c"));
            b.notifyListeners(i(10, 11), i(), i());
        });

        final DynamicTable ex2a = newTable(longCol("ID", 4, 4, 5, 5), intCol("Sentinel", 107, 108, 109, 110), col("Key", "a", "a", "b", "b"));
        final DynamicTable ex2b = newTable(longCol("Ego", 4, 4, 5), intCol("Sentinel", 205, 206, 207), col("Klyuch", "a", "a", "b"));

        assertEquals("", TableTools.diff(fa, ex2a, 10));
        assertEquals("", TableTools.diff(fb, ex2b, 10));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(20, 21), longCol("ID", 5, 5), intCol("Sentinel", 111, 112), col("Key", "c", "c"));
            a.notifyListeners(i(20, 21), i(), i());
        });

        final DynamicTable ex3a = newTable(longCol("ID", 4, 4, 5, 5, 5, 5), intCol("Sentinel", 107, 108, 109, 110, 111, 112), col("Key", "a", "a", "b", "b", "c", "c"));
        final DynamicTable ex3b = newTable(longCol("Ego", 4, 4, 5, 5), intCol("Sentinel", 205, 206, 207, 208), col("Klyuch", "a", "a", "b", "c"));

        TableTools.showWithIndex(fa);
        TableTools.showWithIndex(fb);

        assertEquals("", TableTools.diff(fa, ex3a, 10));
        assertEquals("", TableTools.diff(fb, ex3b, 10));


        System.out.println("A before modfications.");
        TableTools.showWithIndex(a, 30);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(a, i(20, 21), longCol("ID", 5, 5), intCol("Sentinel", 113, 114), col("Key", "c", "c"));
            a.notifyListeners(i(), i(), i(20, 21));
        });

        final DynamicTable ex4a = newTable(longCol("ID", 4, 4, 5, 5, 5, 5), intCol("Sentinel", 107, 108, 109, 110, 113, 114), col("Key", "a", "a", "b", "b", "c", "c"));
        assertEquals("", TableTools.diff(fa, ex4a, 10));
    }

    public void testErrorPropagation() {
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1, 1, 2, 2, 3, 3, 4, 4, 5, 5), intCol("Sentinel", 101, 102, 103, 104, 105, 106, 107, 108, 109, 110), col("Key", "a", "a", "b", "b", "a", "a", "a", "a", "b", "b"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("Ego", 0, 0, 2, 2, 4, 4), intCol("Sentinel", 201, 202, 203, 204, 205, 206), col("Klyuch", "a", "a", "b", "b", "a", "a"));

        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder();
        builder.addTable("a", a, "ID", "Key");
        builder.addTable("b", b, "Ego", "Klyuch");
        final TableMap result = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(builder::build);

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        fa.setAttribute("NAME", "a");
        fb.setAttribute("NAME", "b");

        final ErrorListener la = new ErrorListener("fa", (DynamicTable)fa);
        final ErrorListener lb = new ErrorListener("fb", (DynamicTable)fb);

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        allowingError(() -> {
            a.getIndex().remove(1);
            a.notifyListeners(i(), i(1), i());
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
        }, throwables -> {
            TestCase.assertEquals(1, getUpdateErrors().size());
            final Throwable throwable = throwables.get(0);
            TestCase.assertEquals(IllegalStateException.class, throwable.getClass());
            TestCase.assertEquals("Can not process removed rows in SyncTableFilter!", throwable.getMessage());
            return true;
        });

        assertNotNull(la.originalException);
        assertNotNull(lb.originalException);
        assertEquals("Can not process removed rows in SyncTableFilter!", la.originalException.getMessage());
        assertEquals("Can not process removed rows in SyncTableFilter!", lb.originalException.getMessage());
    }

    public void testDependencies() {
        final QueryTable a = TstUtils.testRefreshingTable(longCol("ID", 1), intCol("Sentinel", 101), col("Key", "a"));
        final QueryTable b = TstUtils.testRefreshingTable(longCol("Ego", 0, 1, 1), intCol("Sentinel", 201, 202, 203), col("Klyuch", "a", "a", "b"));

        final SyncTableFilter.Builder builder = new SyncTableFilter.Builder();
        builder.addTable("a", a, "ID");
        builder.addTable("b", b, "Ego");
        final TableMap result = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(builder::build);

        final Table fa = result.get("a");
        final Table fb = result.get("b");

        fa.setAttribute("NAME", "a");
        fb.setAttribute("NAME", "b");


        final Table fau = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> fa.update("SentinelDoubled=Sentinel*2"));
        final Table fbu = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> fb.update("SentinelDoubled=Sentinel*2"));
        final Table joined = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> fau.naturalJoin(fbu, "Key=Klyuch", "SB=Sentinel,SBD=SentinelDoubled"));
        final Table sentSum = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> joined.update("SS=SBD+SentinelDoubled"));

        TableTools.showWithIndex(sentSum);

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        assertTrue(((BaseTable)sentSum).satisfied(LogicalClock.DEFAULT.currentStep()));
        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        addToTable(a, i(1), longCol("ID", 1), intCol("Sentinel", 102), col("Key", "b"));
        a.notifyListeners(i(1), i(), i());
        assertFalse(((BaseTable)fa).satisfied(LogicalClock.DEFAULT.currentStep()));
        assertFalse(((BaseTable)fb).satisfied(LogicalClock.DEFAULT.currentStep()));
        assertFalse(((BaseTable)sentSum).satisfied(LogicalClock.DEFAULT.currentStep()));

        while (!((BaseTable) fa).satisfied(LogicalClock.DEFAULT.currentStep())) {
            LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
        }
        assertTrue(((BaseTable)fa).satisfied(LogicalClock.DEFAULT.currentStep()));
        LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
        assertTrue(((BaseTable)fb).satisfied(LogicalClock.DEFAULT.currentStep()));

        assertFalse(((BaseTable)joined).satisfied(LogicalClock.DEFAULT.currentStep()));

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TableTools.showWithIndex(sentSum);
        int [] actual = (int[]) sentSum.getColumn("SS").getDirect();
        int [] expected = new int[]{606, 610};
        assertEquals(expected, actual);
    }

    public void testTableMap() {
        final QueryTable source1 = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(10),
                col("Partition", "A", "A", "B", "B", "C", "C", "C", "D", "D", "D"),
                longCol("ID", 1, 2, /* B */ 1, 2, /* C */ 1, 2, 2, /* D */ 1, 2, 2),
                intCol("Sentinel", 101, 102, 103, 104, 105, 106, 107, 108, 109, 110)
        );

        final QueryTable source2 = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(5),
                col("Division", "A", "A", "B", "C", "C"),
                longCol("ID", 2, 3, 1, 2, 2),
                intCol("Sentinel", 201, 202, 203, 204, 205));

        final TableMap sm1 = source1.updateView("SK1=k").byExternal("Partition");
        final TableMap sm2 = source2.updateView("SK2=k").byExternal("Division");

        final TableMap bykey = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> new SyncTableFilter.Builder().addTable("source1", source1, "ID", "Partition").addTable("source2", source2, "ID", "Division").build());
        final Table s1f = bykey.get("source1");
        final Table s2f = bykey.get("source2");
        TableTools.showWithIndex(s1f);
        TableTools.showWithIndex(s2f);

        final TableMap filteredByPartition = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> new SyncTableFilter.Builder("ID").addTableMap("source1", sm1).addTableMap("source2", sm2, "ID").build());
        for (Object key: filteredByPartition.getKeySet()) {
            System.out.println(key);
            TableTools.showWithIndex(filteredByPartition.get(key));
        }

        final TableMap s1tm = new FilteredTableMap(filteredByPartition, sk -> ((SmartKey)sk).get(1).equals("source1"), sk -> ((SmartKey)sk).get(0));
        final TableMap s2tm = new FilteredTableMap(filteredByPartition, sk -> ((SmartKey)sk).get(1).equals("source2"), sk -> ((SmartKey)sk).get(0));

        final Table s1merged = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(s1tm::merge);
        final Table s2merged = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(s2tm::merge);
        final Table s1mergedSorted = s1merged.sort("SK1").dropColumns("SK1");
        final Table s2mergedSorted = s2merged.sort("SK2").dropColumns("SK2");

        assertTableEquals(s1f, s1mergedSorted);
        assertTableEquals(s2f, s2mergedSorted);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(source2, i(10, 11), col("Division", "D", "B"), longCol("ID", 2, 2), intCol("Sentinel", 206, 207));
            source2.notifyListeners(i(10, 11), i(), i());
        });

        assertTableEquals(s1f, s1mergedSorted);
        assertTableEquals(s2f, s2mergedSorted);

        TableTools.showWithIndex(s1f);
        TableTools.showWithIndex(s2f);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(source2, i(12, 13), col("Division", "D", "E"), longCol("ID", 3, 3), intCol("Sentinel", 208, 209));
            source2.notifyListeners(i(12, 13), i(), i());
            addToTable(source1, i(10, 11, 12), col("Partition", "D", "D", "E"), longCol("ID", 3, 4, 3), intCol("Sentinel", 111, 112, 113));
            source1.notifyListeners(i(10, 11, 12), i(), i());
        });

        assertTableEquals(s1f, s1mergedSorted);
        assertTableEquals(s2f, s2mergedSorted);
    }

    public void testTableMapRandomized() {
        for (int seed = 0; seed < 1; ++seed) {
            testTableMapRandomized(seed);
        }
    }

    private void testTableMapRandomized(int seed) {
        LiveTableMonitor.DEFAULT.resetForUnitTests(false, true, seed, 4, 10, 5);

        final Random random = new Random(seed);
        final ColumnInfo[] columnInfo1;
        final ColumnInfo[] columnInfo2;
        final ColumnInfo[] columnInfoSet1;
        final ColumnInfo[] columnInfoSet2;
        final int size = 10;
        final QueryTable source1Unfiltered = getTable(size, random, columnInfo1 = initColumnInfos(new String[]{"Partition", "ID", "Sentinel", "Truthy"},
                new SetGenerator<>("a", "b","c","d", "e"),
                new IncreasingSortedLongGenerator(2, 1000),
                new IntGenerator(0, 1000000),
                new BooleanGenerator()));

        final QueryTable source2Unfiltered = getTable(size, random, columnInfo2 = initColumnInfos(new String[]{"Partition", "ID", "Sentinel", "Truthy"},
                new SetGenerator<>("a", "b","c","d", "e"),
                new IncreasingSortedLongGenerator(2, 1000),
                new IntGenerator(0, 1000000),
                new BooleanGenerator()));

        final QueryTable filterSet1 = getTable(1, random, columnInfoSet1 = initColumnInfos(new String []{"Partition"}, new SetGenerator<>("a", "b", "c", "d", "e")));
        final QueryTable filterSet2 = getTable(1, random, columnInfoSet2 = initColumnInfos(new String []{"Partition"}, new SetGenerator<>("a", "b", "c", "d", "e")));

        final Table dummy = TableTools.newTable(col("Partition", "A"), longCol("ID", 0), intCol("Sentinel", 12345678), col("Truthy", true));

        final Table source1 = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> TableTools.merge(dummy, source1Unfiltered.whereIn(filterSet1, "Partition").update("Truthy=!!Truthy")));
        final Table source2 = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> TableTools.merge(dummy, source2Unfiltered.whereIn(filterSet2, "Partition").update("Truthy=!!Truthy")));

        final TableMap sm1 = source1.updateView("SK1=k").byExternal("Partition");
        final TableMap sm2 = source2.updateView("SK2=k").byExternal("Partition");

        final TableMap bykey = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> new SyncTableFilter.Builder("ID", "Partition").addTable("source1", source1).addTable("source2", source2).build());
        final Table s1f = bykey.get("source1");
        final Table s2f = bykey.get("source2");

        final TableMap bykey2 = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> new SyncTableFilter.Builder("ID", "Partition", "Truthy").addTable("source1", source1).addTable("source2", source2).build());
        final Table s1fKeyed = bykey2.get("source1");
        final Table s2fKeyed = bykey2.get("source2");

        final TableMap filteredByPartition = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> new SyncTableFilter.Builder("ID").addTableMap("source1", sm1).addTableMap("source2", sm2).build());
        final TableMap filteredByPartitionKeyed = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> new SyncTableFilter.Builder("ID", "Truthy").addTableMap("source1", sm1).addTableMap("source2", sm2).build());

        final TableMap s1tm = new FilteredTableMap(filteredByPartition, sk -> ((SmartKey)sk).get(1).equals("source1"), sk -> ((SmartKey)sk).get(0));
        final TableMap s2tm = new FilteredTableMap(filteredByPartition, sk -> ((SmartKey)sk).get(1).equals("source2"), sk -> ((SmartKey)sk).get(0));

        final TableMap s1tmKeyed = new FilteredTableMap(filteredByPartitionKeyed, sk -> ((SmartKey)sk).get(1).equals("source1"), sk -> ((SmartKey)sk).get(0));
        final TableMap s2tmKeyed = new FilteredTableMap(filteredByPartitionKeyed, sk -> ((SmartKey)sk).get(1).equals("source2"), sk -> ((SmartKey)sk).get(0));

        final Table s1merged = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(s1tm::merge);
        final Table s2merged = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(s2tm::merge);
        final Table s1mergedSorted = s1merged.sort("SK1").dropColumns("SK1");
        final Table s2mergedSorted = s2merged.sort("SK2").dropColumns("SK2");
        
        final Table s1KeyedMerged = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(s1tmKeyed::merge);
        final Table s2KeyedMerged = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(s2tmKeyed::merge);
        final Table s1KeyedMergedSorted = s1KeyedMerged.sort("SK1").dropColumns("SK1");
        final Table s2KeyedMergedSorted = s2KeyedMerged.sort("SK2").dropColumns("SK2");

        assertTableEquals(s1f, s1mergedSorted);
        assertTableEquals(s2f, s2mergedSorted);

        assertTableEquals(s1fKeyed, s1KeyedMergedSorted);
        assertTableEquals(s2fKeyed, s2KeyedMergedSorted);

        for (int step = 0; step < 100; ++step) {
            if (LiveTableTestCase.printTableUpdates) {
                System.out.println("Seed = " + seed + ", step=" + step);
            }
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                if (random.nextInt(10) == 0) {
                    GenerateTableUpdates.generateAppends(size, random, filterSet1, columnInfoSet1);
                }
                if (random.nextInt(10) == 0) {
                    GenerateTableUpdates.generateAppends(size, random, filterSet2, columnInfoSet2);
                }
                // append to table 1
                GenerateTableUpdates.generateAppends(size, random, source1Unfiltered, columnInfo1);
                // append to table 2
                GenerateTableUpdates.generateAppends(size / 2, random, source2Unfiltered, columnInfo2);
            });

            if (printTableUpdates) {
                System.out.println("Source 1 (tm)");
                TableTools.showWithIndex(s1merged);
                System.out.println("Source 2 (tm)");
                TableTools.showWithIndex(s2merged);

                System.out.println("Source 1 Keyed (tm)");
                TableTools.showWithIndex(s1KeyedMerged);
                System.out.println("Source 2 (tm)");
                TableTools.showWithIndex(s2KeyedMerged);
            }

            assertTableEquals(s1f, s1mergedSorted);
            assertTableEquals(s2f, s2mergedSorted);

            assertTableEquals(s1fKeyed, s1KeyedMergedSorted);
            assertTableEquals(s2fKeyed, s2KeyedMergedSorted);
        }
    }

    private static class ErrorListener extends InstrumentedListenerAdapter {
        Throwable originalException;

        ErrorListener(String description, DynamicTable table) {
            super("Error Checker: " + description, table, false);
            table.listenForUpdates(this, false);
        }

        @Override
        public void onUpdate(Index added, Index removed, Index modified) {
            fail("Should not have gotten an update!");
        }

        @Override
        public void onFailureInternal(Throwable originalException, UpdatePerformanceTracker.Entry sourceEntry) {
            this.originalException = originalException;
        }
    }
}
