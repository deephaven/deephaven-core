package io.deephaven.db.v2;

import io.deephaven.base.Function;
import io.deephaven.base.SleepUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.SystemicObjectTracker;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.util.liveness.SingletonLivenessManager;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;

import java.util.*;
import java.util.stream.IntStream;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.v2.TstUtils.*;
import static io.deephaven.db.v2.TstUtils.c;
import static io.deephaven.db.v2.TstUtils.i;

@Category(OutOfBandTest.class)
public class TableMapTest extends LiveTableTestCase {

    private boolean oldCheckLtm;

    @Override
    protected void setUp() throws Exception {
        if (null == ProcessEnvironment.tryGet()) {
            ProcessEnvironment.basicServerInitialization(Configuration.getInstance(),
                "TestTransformableTableMapThenMerge", new StreamLoggerImpl());
        }
        super.setUp();
        setExpectError(false);
        oldCheckLtm = LiveTableMonitor.DEFAULT.setCheckTableOperations(false);
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            LiveTableMonitor.DEFAULT.setCheckTableOperations(oldCheckLtm);
        }
    }

    public void testMergeSimple() {
        final QueryTable queryTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6),
            c("Sym", "aa", "bb", "aa", "bb"),
            c("intCol", 10, 20, 40, 60),
            c("doubleCol", 0.1, 0.2, 0.4, 0.6));

        final Table withK = queryTable.update("K=k");

        final TableMap tableMap = withK.byExternal("Sym");
        final Table merged = tableMap.merge();
        final Table mergedByK = merged.sort("K");

        if (printTableUpdates) {
            TableTools.show(withK);
            TableTools.show(mergedByK);
        }

        assertEquals("", TableTools.diff(mergedByK, withK, 10));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3, 9), c("Sym", "cc", "cc"), c("intCol", 30, 90),
                c("doubleCol", 2.3, 2.9));
            queryTable.notifyListeners(i(3, 9), i(), i());
        });

        if (printTableUpdates) {
            TableTools.show(withK);
            TableTools.show(merged);
            TableTools.show(mergedByK);
        }

        assertEquals("", TableTools.diff(mergedByK, withK, 10));
    }

    public void testMergePopulate() {
        final QueryTable queryTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6),
            c("Sym", "aa", "bb", "aa", "bb"),
            c("intCol", 10, 20, 40, 60),
            c("doubleCol", 0.1, 0.2, 0.4, 0.6));

        final Table withK = queryTable.update("K=k");

        final TableMap tableMap = withK.byExternal("Sym");
        tableMap.populateKeys("cc", "dd");

        final Table merged = tableMap.merge();
        final Table mergedByK = merged.sort("K");

        if (printTableUpdates) {
            TableTools.show(withK);
            TableTools.show(mergedByK);
        }

        assertEquals("", TableTools.diff(mergedByK, withK, 10));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3, 9), c("Sym", "cc", "cc"), c("intCol", 30, 90),
                c("doubleCol", 2.3, 2.9));
            queryTable.notifyListeners(i(3, 9), i(), i());
        });

        if (printTableUpdates) {
            TableTools.show(withK);
            TableTools.show(mergedByK);
        }

        assertEquals("", TableTools.diff(mergedByK, withK, 10));
    }

    public void testMergeIncremental() {
        for (int seed = 1; seed < 2; ++seed) {
            testMergeIncremental(seed);
        }
    }

    private void testMergeIncremental(int seed) {
        final Random random = new Random(seed);

        final int size = 100;

        final TstUtils.ColumnInfo[] columnInfo;
        final String[] syms = {"aa", "bb", "cc", "dd"};
        final QueryTable table = getTable(size, random,
            columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                new SetGenerator<>(syms),
                new TstUtils.IntGenerator(0, 20),
                new TstUtils.DoubleGenerator(0, 100)));

        final EvalNugget en[] = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return table.byExternal("Sym").populateKeys((Object[]) syms).merge()
                            .sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.byExternal("intCol")
                            .populateKeys(
                                IntStream.rangeClosed(0, 20).boxed().toArray(Object[]::new))
                            .merge().sort("intCol");
                    }
                },
        };

        for (int step = 0; step < 100; step++) {
            if (printTableUpdates) {
                System.out.println("Seed =" + seed + ", step = " + step);
            }
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    static class SizeNugget implements EvalNuggetInterface {
        final Table originalTable;
        final Table computedTable;

        SizeNugget(Table originalTable, Table computedTable) {
            this.originalTable = originalTable;
            this.computedTable = computedTable;
        }


        @Override
        public void validate(String msg) {
            Assert.equals(originalTable.size(), "originalTable.size()", computedTable.size(),
                "computedTable.size()");
        }

        @Override
        public void show() {
            TableTools.showWithIndex(originalTable);
        }
    }

    public void testAsTable() {
        final Random random = new Random(0);

        final int size = 100;

        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable table = getTable(size, random,
            columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol", "Keys"},
                new TstUtils.SetGenerator<>("aa", "bb", "cc", "dd"),
                new TstUtils.IntGenerator(0, 20),
                new TstUtils.DoubleGenerator(0, 100),
                new TstUtils.SortedLongGenerator(0, Long.MAX_VALUE - 1)));

        final Table withK = table.update("K=Keys");

        final QueryTable rightTable = getTable(size, random,
            initColumnInfos(new String[] {"Sym", "RightCol"},
                new TstUtils.SetGenerator<>("aa", "bb", "cc", "dd"),
                new TstUtils.IntGenerator(100, 200)));

        final TableMap map = withK.byExternal("Sym");
        map.populateKeys("aa", "bb", "cc", "dd");
        final Table asTable = map.asTable(false, true, false);

        final TableMap rightMap = rightTable.byExternal("Sym");
        rightMap.populateKeys("aa", "bb", "cc", "dd");
        final Table rightAsTable = rightMap.asTable(false, true, false);

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return ((TransformableTableMap) table.update("K=Keys").byExternal("Sym")
                            .populateKeys("aa", "bb", "cc", "dd").asTable(false, false, false)
                            .update("K2=Keys*2").select("K", "K2", "Half=doubleCol/2",
                                "Sq=doubleCol*doubleCol", "Weight=intCol*doubleCol", "Sym")).merge()
                                    .sort("K", "Sym");
                    }
                },
                new SizeNugget(table, asTable),
                new QueryTableTest.TableComparator(
                    withK.naturalJoin(rightTable.lastBy("Sym"), "Sym").sort("K", "Sym"),
                    asTable.naturalJoin(rightTable.lastBy("Sym"), "Sym").coalesce().sort("K",
                        "Sym")),
                new QueryTableTest.TableComparator(
                    withK.naturalJoin(rightTable.lastBy("Sym"), "Sym").sort("K", "Sym"),
                    asTable.naturalJoin(rightAsTable.lastBy(), "Sym").coalesce().sort("K", "Sym")),
        };

        for (int i = 0; i < 100; i++) {
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    public void testTransformTableMapThenMerge() {
        LiveTableMonitor.DEFAULT.resetForUnitTests(false, true, 0, 4, 10, 5);

        final QueryTable sourceTable = TstUtils.testRefreshingTable(i(1), intCol("Key", 1),
            intCol("Sentinel", 1), col("Sym", "a"), doubleCol("DoubleCol", 1.1));

        final TableMap tableMap = sourceTable.byExternal("Key");

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return tableMap.merge().sort("Key");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return tableMap.transformTables(t -> t.update("K2=Key * 2")
                            .update("K3=Key + K2").update("K5 = K3 + K2")).merge().sort("Key");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return tableMap.transformTablesWithMap(tableMap,
                            (l, r) -> l.naturalJoin(r.lastBy("Key"), "Key", "Sentinel2=Sentinel"))
                            .merge().sort("Key");
                    }
                }
        };

        for (int ii = 0; ii < 100; ++ii) {
            final int iteration = ii + 1;
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                final long baseLocation = iteration * 10;
                final Index addIndex =
                    Index.FACTORY.getIndexByRange(baseLocation, baseLocation + 4);
                final int[] sentinels = {iteration * 5, iteration * 5 + 1, iteration * 5 + 2,
                        iteration * 5 + 3, iteration * 5 + 4};
                addToTable(sourceTable, addIndex,
                    intCol("Key", 1, 3, iteration, iteration - 1, iteration * 2),
                    intCol("Sentinel", sentinels), col("Sym", "aa", "bb", "cc", "dd", "ee"),
                    doubleCol("DoubleCol", 2.2, 3.3, 4.4, 5.5, 6.6));
                sourceTable.notifyListeners(addIndex, i(), i());
                if (printTableUpdates) {
                    System.out
                        .println("Source Table, iteration=" + iteration + ", added=" + addIndex);
                    TableTools.showWithIndex(sourceTable);
                }
            });
            TstUtils.validate("iteration = " + iteration, en);
        }
    }

    public void testAttributes() {
        final QueryTable queryTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6),
            c("Sym", "aa", "bb", "aa", "bb"),
            c("intCol", 10, 20, 40, 60),
            c("doubleCol", 0.1, 0.2, 0.4, 0.6));

        queryTable.setAttribute(Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar");

        final TableMap tableMap = queryTable.byExternal("Sym");

        for (Table table : tableMap.values()) {
            table.setAttribute("quux", "baz");
        }

        final Table asTable = tableMap.asTable(true, false, true);
        if (SystemicObjectTracker.isSystemicObjectMarkingEnabled()) {
            TestCase.assertEquals(CollectionUtil.mapFromArray(String.class, Object.class, "quux",
                "baz", Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar", Table.SYSTEMIC_TABLE_ATTRIBUTE,
                Boolean.TRUE), asTable.getAttributes());
        } else {
            TestCase.assertEquals(CollectionUtil.mapFromArray(String.class, Object.class, "quux",
                "baz", Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar"), asTable.getAttributes());
        }

        Table merged = ((TransformableTableMap) asTable).merge();
        if (SystemicObjectTracker.isSystemicObjectMarkingEnabled()) {
            TestCase.assertEquals(CollectionUtil.mapFromArray(String.class, Object.class, "quux",
                "baz", Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar", Table.MERGED_TABLE_ATTRIBUTE, true,
                Table.SYSTEMIC_TABLE_ATTRIBUTE, Boolean.TRUE), merged.getAttributes());
        } else {
            TestCase.assertEquals(
                CollectionUtil.mapFromArray(String.class, Object.class, "quux", "baz",
                    Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar", Table.MERGED_TABLE_ATTRIBUTE, true),
                merged.getAttributes());
        }

        int tableCounter = 1;
        for (Table table : tableMap.values()) {
            table.setAttribute("differing", tableCounter++);
        }

        // the proxy blows up
        try {
            asTable.getAttributes();
            TestCase.fail("Get attributes is inconsistent!");
        } catch (IllegalArgumentException e) {
            TestCase.assertEquals("Underlying tables do not have consistent attributes.",
                e.getMessage());
        }

        // the merged table just takes the set that is consistent
        merged = ((TransformableTableMap) asTable).merge();
        if (SystemicObjectTracker.isSystemicObjectMarkingEnabled()) {
            TestCase.assertEquals(CollectionUtil.mapFromArray(String.class, Object.class, "quux",
                "baz", Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar", Table.MERGED_TABLE_ATTRIBUTE, true,
                Table.SYSTEMIC_TABLE_ATTRIBUTE, Boolean.TRUE), merged.getAttributes());
        } else {
            TestCase.assertEquals(
                CollectionUtil.mapFromArray(String.class, Object.class, "quux", "baz",
                    Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar", Table.MERGED_TABLE_ATTRIBUTE, true),
                merged.getAttributes());
        }
    }

    public void testJoinSanity() {
        final QueryTable left = TstUtils.testRefreshingTable(i(1, 2, 4, 6),
            c("USym", "aa", "bb", "aa", "bb"),
            c("Sym", "aa_1", "bb_1", "aa_2", "bb_2"),
            c("LeftSentinel", 10, 20, 40, 60));
        final QueryTable right = TstUtils.testRefreshingTable(i(3, 5, 7, 9),
            c("USym", "aa", "bb", "aa", "bb"),
            c("Sym", "aa_1", "bb_1", "aa_2", "bb_2"),
            c("RightSentinel", 30, 50, 70, 90));

        final TableMap leftMap = left.byExternal("USym");
        final TableMap rightMap = right.byExternal("USym");

        final Table leftAsTable =
            leftMap.asTableBuilder().sanityCheckJoin(true).allowCoalesce(false).build();
        final Table rightAsTable =
            rightMap.asTableBuilder().sanityCheckJoin(true).allowCoalesce(false).build();

        final Table result = leftAsTable.join(rightAsTable, "Sym", "RightSentinel");

        final Table mergedResult = ((TransformableTableMap) result).merge();
        TableTools.show(mergedResult);

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        addToTable(left, i(8), c("USym", "bb"), c("Sym", "aa_1"), c("LeftSentinel", 80));

        allowingError(() -> {
            left.notifyListeners(i(8), i(), i());
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
        }, throwables -> {
            TestCase.assertEquals(1, getUpdateErrors().size());
            final Throwable throwable = throwables.get(0);
            TestCase.assertEquals(IllegalArgumentException.class, throwable.getClass());
            TestCase.assertEquals(
                "join([Sym]) Left join key \"aa_1\" exists in multiple TableMap keys, \"aa\" and \"bb\"",
                throwable.getMessage());
            return true;
        });

    }

    public void testDependencies() {
        final QueryTable sourceTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6),
            c("USym", "aa", "bb", "aa", "bb"),
            c("Sentinel", 10, 20, 40, 60));

        final TableMap result = sourceTable.byExternal("USym");
        final Table aa = result.get("aa");
        final Table aa2 = aa.update("S2=Sentinel * 2");
        TableTools.show(aa2);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> TestCase
            .assertTrue(((QueryTable) aa2).satisfied(LogicalClock.DEFAULT.currentStep())));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(sourceTable, i(8), c("USym", "bb"), c("Sentinel", 80));
            sourceTable.notifyListeners(i(8), i(), i());
            TestCase.assertFalse(((QueryTable) aa2).satisfied(LogicalClock.DEFAULT.currentStep()));
            // We need to flush one notification: one for the source table because we do not require
            // an intermediate view table in this case
            final boolean flushed = LiveTableMonitor.DEFAULT.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);
            TestCase.assertTrue(((QueryTable) aa2).satisfied(LogicalClock.DEFAULT.currentStep()));
        });
    }

    public static class PauseHelper {
        private final long start = System.currentTimeMillis();
        private volatile boolean released = false;

        @SuppressWarnings("unused")
        public <T> T pauseValue(T retVal) {
            System.out
                .println((System.currentTimeMillis() - start) / 1000.0 + ": Reading: " + retVal);

            synchronized (this) {
                while (!released) {
                    try {
                        System.out.println((System.currentTimeMillis() - start) / 1000.0
                            + ": Waiting for release of: " + retVal);
                        wait(5000);
                        if (!released) {
                            TestCase.fail("Not released!");
                        }
                        System.out.println((System.currentTimeMillis() - start) / 1000.0
                            + ": Release of: " + retVal);
                    } catch (InterruptedException e) {
                        TestCase.fail("Interrupted!");
                    }
                }
            }

            return retVal;
        }

        synchronized void release() {
            System.out.println((System.currentTimeMillis() - start) / 1000.0 + ": Releasing.");
            released = true;
            notifyAll();
        }

        synchronized void pause() {
            released = false;
        }
    }

    public void testCrossDependencies() {
        LiveTableMonitor.DEFAULT.resetForUnitTests(false, true, 0, 2, 0, 0);

        final QueryTable sourceTable = TstUtils.testRefreshingTable(i(1, 2),
            c("USym", "aa", "bb"),
            c("Sentinel", 10, 20));

        final QueryTable sourceTable2 = TstUtils.testRefreshingTable(i(3, 5),
            c("USym2", "aa", "bb"),
            c("Sentinel2", 30, 50));

        final TableMap result = sourceTable.byExternal("USym");

        final PauseHelper pauseHelper = new PauseHelper();
        final PauseHelper pauseHelper2 = new PauseHelper();
        QueryScope.addParam("pauseHelper", pauseHelper);
        QueryScope.addParam("pauseHelper2", pauseHelper);

        pauseHelper.release();
        pauseHelper2.release();

        final TableMap result2 =
            sourceTable2.update("SlowItDown=pauseHelper.pauseValue(k)").byExternal("USym2")
                .transformTables(t -> t.update("SlowItDown2=pauseHelper2.pauseValue(2 * k)"));

        // pauseHelper.pause();
        pauseHelper2.pause();

        final TableMap joined = result.transformTablesWithMap(result2, (l, r) -> {
            System.out.println("Doing naturalJoin");
            return l.naturalJoin(r, "USym=USym2");
        });
        final Table merged = joined.merge();

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        addToTable(sourceTable, i(3), c("USym", "cc"), c("Sentinel", 30));
        addToTable(sourceTable2, i(7, 9), c("USym2", "cc", "dd"), c("Sentinel2", 70, 90));
        System.out.println("Launching Notifications");
        sourceTable.notifyListeners(i(3), i(), i());
        sourceTable2.notifyListeners(i(7, 9), i(), i());

        System.out.println("Waiting for notifications");

        new Thread(() -> {
            System.out.println("Sleeping before release.");
            SleepUtil.sleep(1000);
            System.out.println("Doing release.");
            pauseHelper.release();
            pauseHelper2.release();
            System.out.println("Released.");
        }).start();

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        pauseHelper2.pause();

        System.out.println("Before second cycle.");
        TableTools.showWithIndex(sourceTable);
        TableTools.showWithIndex(sourceTable2);

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        addToTable(sourceTable, i(4, 5), c("USym", "cc", "dd"), c("Sentinel", 40, 50));
        addToTable(sourceTable2, i(8, 10), c("USym2", "cc", "dd"), c("Sentinel2", 80, 100));
        removeRows(sourceTable2, i(7, 9));

        System.out.println("Launching Notifications");
        sourceTable.notifyListeners(i(4, 5), i(), i());
        sourceTable2.notifyListeners(i(8, 10), i(7, 9), i());

        System.out.println("Waiting for notifications");

        new Thread(() -> {
            System.out.println("Sleeping before release.");
            SleepUtil.sleep(1000);
            System.out.println("Doing release.");
            pauseHelper.release();
            pauseHelper2.release();
            System.out.println("Released.");
        }).start();

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();



        TableTools.showWithIndex(merged);
    }

    public void testCrossDependencies2() {
        LiveTableMonitor.DEFAULT.resetForUnitTests(false, true, 0, 2, 0, 0);

        final QueryTable sourceTable = TstUtils.testRefreshingTable(i(1, 2),
            c("USym", "aa", "bb"),
            c("Sentinel", 10, 20));

        final QueryTable sourceTable2 = TstUtils.testRefreshingTable(i(3, 5, 9),
            c("USym2", "aa", "bb", "dd"),
            c("Sentinel2", 30, 50, 90));

        final TableMap result = sourceTable.byExternal("USym");

        final PauseHelper pauseHelper = new PauseHelper();
        QueryScope.addParam("pauseHelper", pauseHelper);

        pauseHelper.release();

        final TableMap result2 = sourceTable2.byExternal("USym2")
            .transformTables(t -> t.update("SlowItDown2=pauseHelper.pauseValue(2 * k)"));

        final TableMap joined = result.transformTablesWithMap(result2, (l, r) -> {
            System.out.println("Doing naturalJoin");
            return l.naturalJoin(r, "USym=USym2");
        });
        final Table merged = joined.merge();


        pauseHelper.pause();
        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        addToTable(sourceTable, i(5), c("USym", "dd"), c("Sentinel", 50));
        addToTable(sourceTable2, i(10), c("USym2", "dd"), c("Sentinel2", 100));
        removeRows(sourceTable2, i(9));

        System.out.println("Launching Notifications");
        sourceTable.notifyListeners(i(5), i(), i());
        sourceTable2.notifyListeners(i(10), i(9), i());

        System.out.println("Waiting for notifications");

        new Thread(() -> {
            System.out.println("Sleeping before release.");
            SleepUtil.sleep(1000);
            System.out.println("Doing release.");
            pauseHelper.release();
            System.out.println("Released.");
        }).start();

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();

        TableTools.showWithIndex(merged);
    }

    public void testTableMapScope() {
        testTableMapScope(true);
        testTableMapScope(false);
    }

    private void testTableMapScope(boolean refreshing) {
        final DynamicTable table =
            TableTools.newTable(TableTools.col("Key", "A", "B"), intCol("Value", 1, 2));
        if (refreshing) {
            table.setRefreshing(true);
        }

        final SafeCloseable scopeCloseable = LivenessScopeStack.open();

        final TableMap map = table.byExternal("Key");
        final Table value = map.get("A");
        assertNotNull(value);

        final SingletonLivenessManager mapManager = new SingletonLivenessManager(map);

        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(scopeCloseable::close);

        if (refreshing) {
            org.junit.Assert.assertTrue(map.tryRetainReference());
            org.junit.Assert.assertTrue(value.tryRetainReference());

            map.dropReference();
            value.dropReference();
        }

        final SafeCloseable scopeCloseable2 = LivenessScopeStack.open();
        final Table valueAgain = map.get("A");
        assertSame(value, valueAgain);
        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(scopeCloseable2::close);

        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(mapManager::release);

        org.junit.Assert.assertFalse(value.tryRetainReference());
        org.junit.Assert.assertFalse(map.tryRetainReference());
    }

    private void testMemoize(Table source, Function.Unary<TableMap, Table> op) {
        testMemoize(source, op, op);
    }

    private void testMemoize(Table source, Function.Unary<TableMap, Table> op,
        Function.Unary<TableMap, Table> op2) {
        final TableMap result = op.call(source);
        final TableMap result2 = op2.call(source);
        org.junit.Assert.assertSame(result, result2);
    }

    private void testNoMemoize(Table source, Function.Unary<TableMap, Table> op,
        Function.Unary<TableMap, Table> op2) {
        final TableMap result = op.call(source);
        final TableMap result2 = op2.call(source);
        org.junit.Assert.assertNotSame(result, result2);
    }

    public void testMemoize() {
        final QueryTable sourceTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6),
            c("USym", "aa", "bb", "aa", "bb"),
            c("Sentinel", 10, 20, 40, 60));

        final boolean old = QueryTable.setMemoizeResults(true);
        try {
            testMemoize(sourceTable, t -> t.byExternal("USym"));
            testMemoize(sourceTable, t -> t.byExternal("Sentinel"));
            testMemoize(sourceTable, t -> t.byExternal(true, "USym"));
            testMemoize(sourceTable, t -> t.byExternal(true, "Sentinel"));
            testMemoize(sourceTable, t -> t.byExternal(false, "Sentinel"),
                t -> t.byExternal("Sentinel"));
            testNoMemoize(sourceTable, t -> t.byExternal(true, "Sentinel"),
                t -> t.byExternal("Sentinel"));
            testNoMemoize(sourceTable, t -> t.byExternal("USym"), t -> t.byExternal("Sentinel"));
        } finally {
            QueryTable.setMemoizeResults(old);
        }
    }

    public void testTableMapSupplierListeners() {
        final QueryTable base = TstUtils.testRefreshingTable(i(0, 1, 2, 3, 4, 5),
            stringCol("Key", "Zero", "Zero", "One", "One", "One", "One"),
            stringCol("Color", "Red", "Blue", "Red", "Blue", "Red", "Blue"),
            intCol("Value", -1, 0, 1, 2, 3, 4));

        final TableMap byKey = base.byExternal("Key");
        final TableMapSupplier supplier =
            new TableMapSupplier(byKey, Collections.singletonList(t -> t.where("Color=`Red`")));

        assertTableEquals(base.where("Color=`Red`"), supplier.merge());

        final Map<String, Table> listenerResults = new HashMap<>();

        supplier.addListener((key, table) -> listenerResults.put((String) key, table));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index idx = i(6, 7, 8, 9);
            addToTable(base, idx,
                stringCol("Key", "Two", "Two", "Two", "Two"),
                stringCol("Color", "Red", "Blue", "Red", "Blue"),
                intCol("Value", 5, 6, 7, 8));
            base.notifyListeners(idx, i(), i());
        });

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index idx = i(10, 11, 12, 13);
            addToTable(base, idx,
                stringCol("Key", "Three", "Three", "Three", "Three"),
                stringCol("Color", "Red", "Red", "Red", "Blue"),
                intCol("Value", 9, 10, 11, 12));
            base.notifyListeners(idx, i(), i());
        });

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index idx = i(14, 15, 16, 17);
            addToTable(base, idx,
                stringCol("Key", "Four", "Four", "Four", "Four"),
                stringCol("Color", "Blue", "Blue", "Blue", "Blue"),
                intCol("Value", 13, 14, 15, 16));
            base.notifyListeners(idx, i(), i());
        });

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index idx = i(18, 19, 20, 21);
            addToTable(base, idx,
                stringCol("Key", "Four", "Four", "Four", "Four"),
                stringCol("Color", "Blue", "Blue", "Blue", "Blue"),
                intCol("Value", 9, 10, 11, 12));
            base.notifyListeners(idx, i(), i());
        });

        assertTableEquals(base.where("Key=`Two`", "Color=`Red`"), listenerResults.get("Two"));
        assertTableEquals(base.where("Key=`Three`", "Color=`Red`"), listenerResults.get("Three"));
        assertTableEquals(base.where("Key=`Four`", "Color=`Red`"), listenerResults.get("Four"));

    }
}
