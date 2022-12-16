/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.base.SleepUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.*;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.generator.SortedLongGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.process.ProcessEnvironment;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

@Category(OutOfBandTest.class)
public class PartitionedTableTest extends RefreshingTableTestCase {

    @Override
    public void setUp() throws Exception {
        if (null == ProcessEnvironment.tryGet()) {
            ProcessEnvironment.basicServerInitialization(Configuration.getInstance(),
                    "TestTransformablePartitionedTableThenMerge", new StreamLoggerImpl());
        }
        super.setUp();
        setExpectError(false);
    }

    public void testMergeSimple() {
        final QueryTable queryTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                c("Sym", "aa", "bb", "aa", "bb"),
                c("intCol", 10, 20, 40, 60),
                c("doubleCol", 0.1, 0.2, 0.4, 0.6));

        final Table withK = queryTable.update("K=k");

        final PartitionedTable partitionedTable = withK.partitionBy("Sym");
        final Table merged = partitionedTable.merge();
        final Table mergedByK = merged.sort("K");

        if (printTableUpdates) {
            TableTools.show(withK);
            TableTools.show(mergedByK);
        }

        assertEquals("", TableTools.diff(mergedByK, withK, 10));

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3, 9), c("Sym", "cc", "cc"), c("intCol", 30, 90), c("doubleCol", 2.3, 2.9));
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
        final QueryTable queryTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                c("Sym", "aa", "bb", "aa", "bb"), c("intCol", 10, 20, 40, 60), c("doubleCol", 0.1, 0.2, 0.4, 0.6));

        final Table withK = queryTable.update("K=k");

        final QueryTable keyTable = testTable(c("Sym", "cc", "dd"));
        final PartitionedTable partitionedTable = withK.partitionedAggBy(List.of(), true, keyTable, "Sym");

        final Table merged = partitionedTable.merge();
        final Table mergedByK = merged.sort("K");

        if (printTableUpdates) {
            TableTools.show(withK);
            TableTools.show(mergedByK);
        }

        assertEquals("", TableTools.diff(mergedByK, withK, 10));

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3, 9), c("Sym", "cc", "cc"), c("intCol", 30, 90), c("doubleCol", 2.3, 2.9));
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

        final ColumnInfo[] columnInfo;
        final String[] syms = {"aa", "bb", "cc", "dd"};
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>(syms),
                        new IntGenerator(0, 20),
                        new DoubleGenerator(0, 100)));

        final EvalNugget en[] = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return table.partitionedAggBy(List.of(), true, testTable(c("Sym", syms)), "Sym")
                                .merge().sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.partitionedAggBy(List.of(), true,
                                testTable(intCol("intCol", IntStream.rangeClosed(0, 20).toArray())), "intCol")
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
            Assert.equals(originalTable.size(), "originalTable.size()", computedTable.size(), "computedTable.size()");
        }

        @Override
        public void show() {
            TableTools.showWithRowSet(originalTable);
        }
    }

    public void testProxy() {
        final Random random = new Random(0);

        final int size = 100;

        final ColumnInfo[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol", "Indices"},
                        new SetGenerator<>("aa", "bb", "cc", "dd"),
                        new IntGenerator(0, 20),
                        new DoubleGenerator(0, 100),
                        new SortedLongGenerator(0, Long.MAX_VALUE - 1)));

        final Table withK = table.update("K=Indices");

        final QueryTable rightTable = getTable(size, random, initColumnInfos(new String[] {"Sym", "RightCol"},
                new SetGenerator<>("aa", "bb", "cc", "dd"),
                new IntGenerator(100, 200)));

        final PartitionedTable leftPT =
                withK.partitionedAggBy(List.of(), true, testTable(c("Sym", "aa", "bb", "cc", "dd")), "Sym");
        final PartitionedTable.Proxy leftProxy = leftPT.proxy(false, false);

        final PartitionedTable rightPT =
                rightTable.partitionedAggBy(List.of(), true, testTable(c("Sym", "aa", "bb", "cc", "dd")), "Sym");
        final PartitionedTable.Proxy rightProxy = rightPT.proxy(false, false);

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return table.update("K=Indices")
                                .partitionedAggBy(List.of(), true, testTable(c("Sym", "aa", "bb", "cc", "dd")), "Sym")
                                .proxy(false, false)
                                .update("K2=Indices*2")
                                .select("K", "K2", "Half=doubleCol/2", "Sq=doubleCol*doubleCol",
                                        "Weight=intCol*doubleCol", "Sym")
                                .target().merge().sort("K", "Sym");
                    }
                },
                new SizeNugget(table, leftProxy.target().merge()),
                new QueryTableTest.TableComparator(withK.naturalJoin(rightTable.lastBy("Sym"), "Sym").sort("K", "Sym"),
                        leftProxy.naturalJoin(rightTable.lastBy("Sym"), "Sym").target().merge().sort("K", "Sym")),
                new QueryTableTest.TableComparator(withK.naturalJoin(rightTable.lastBy("Sym"), "Sym").sort("K", "Sym"),
                        leftProxy.naturalJoin(rightProxy.lastBy(), "Sym").target().merge().sort("K", "Sym")),
        };

        for (int i = 0; i < 100; i++) {
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    public void testTransformPartitionedTableThenMerge() {
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 4, 10, 5);

        final QueryTable sourceTable = testRefreshingTable(i(1).toTracking(),
                intCol("Key", 1), intCol("Sentinel", 1), col("Sym", "a"), doubleCol("DoubleCol", 1.1));

        final PartitionedTable partitionedTable = sourceTable.partitionBy("Key");

        final ExecutionContext executionContext = ExecutionContext.makeExecutionContext(true);
        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return partitionedTable.merge().sort("Key");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return partitionedTable
                                .transform(executionContext,
                                        t -> t.update("K2=Key * 2").update("K3=Key + K2").update("K5 = K3 + K2"))
                                .merge().sort("Key");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return partitionedTable
                                .partitionedTransform(partitionedTable, executionContext,
                                        (l, r) -> l.naturalJoin(r.lastBy("Key"), "Key", "Sentinel2=Sentinel"))
                                .merge().sort("Key");
                    }
                }
        };

        final int iterations = SHORT_TESTS ? 40 : 100;
        for (int ii = 0; ii < iterations; ++ii) {
            final int iteration = ii + 1;
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                final long baseLocation = iteration * 10;
                final RowSet addRowSet = RowSetFactory.fromRange(baseLocation, baseLocation + 4);
                final int[] sentinels =
                        {iteration * 5, iteration * 5 + 1, iteration * 5 + 2, iteration * 5 + 3, iteration * 5 + 4};
                addToTable(sourceTable, addRowSet, intCol("Key", 1, 3, iteration, iteration - 1, iteration * 2),
                        intCol("Sentinel", sentinels), col("Sym", "aa", "bb", "cc", "dd", "ee"),
                        doubleCol("DoubleCol", 2.2, 3.3, 4.4, 5.5, 6.6));
                sourceTable.notifyListeners(addRowSet, i(), i());
                if (printTableUpdates) {
                    System.out.println("Source Table, iteration=" + iteration + ", added=" + addRowSet);
                    TableTools.showWithRowSet(sourceTable);
                }
            });
            validate("iteration = " + iteration, en);
        }
    }

    public void testAttributes() {
        final QueryTable queryTable = testTable(i(1, 2, 4, 6).toTracking(),
                c("Sym", "aa", "bb", "aa", "bb"),
                c("intCol", 10, 20, 40, 60),
                c("doubleCol", 0.1, 0.2, 0.4, 0.6));

        queryTable.setAttribute(Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar");

        final PartitionedTable partitionedTable = queryTable.partitionBy("Sym");

        for (Table table : partitionedTable.constituents()) {
            ((QueryTable) table).setAttribute("quux", "baz");
        }

        final PartitionedTable.Proxy proxy = partitionedTable.proxy(true, true);

        Table merged = proxy.target().merge();
        if (SystemicObjectTracker.isSystemicObjectMarkingEnabled()) {
            TestCase.assertEquals(CollectionUtil.mapFromArray(String.class, Object.class, "quux", "baz",
                    Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar", Table.MERGED_TABLE_ATTRIBUTE, true,
                    Table.SYSTEMIC_TABLE_ATTRIBUTE, Boolean.TRUE), merged.getAttributes());
        } else {
            TestCase.assertEquals(
                    CollectionUtil.mapFromArray(String.class, Object.class, "quux", "baz",
                            Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar", Table.MERGED_TABLE_ATTRIBUTE, true),
                    merged.getAttributes());
        }

        int tableCounter = 1;
        for (Table table : partitionedTable.constituents()) {
            ((QueryTable) table).setAttribute("differing", tableCounter++);
        }

        // the merged table just takes the set that is consistent
        merged = proxy.target().merge();
        if (SystemicObjectTracker.isSystemicObjectMarkingEnabled()) {
            TestCase.assertEquals(CollectionUtil.mapFromArray(String.class, Object.class, "quux", "baz",
                    Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar", Table.MERGED_TABLE_ATTRIBUTE, true,
                    Table.SYSTEMIC_TABLE_ATTRIBUTE, Boolean.TRUE), merged.getAttributes());
        } else {
            TestCase.assertEquals(
                    CollectionUtil.mapFromArray(String.class, Object.class, "quux", "baz",
                            Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar", Table.MERGED_TABLE_ATTRIBUTE, true),
                    merged.getAttributes());
        }
    }

    public void testJoinSanity() {
        final QueryTable left = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                c("USym", "aa", "bb", "aa", "bb"),
                c("Sym", "aa_1", "bb_1", "aa_2", "bb_2"),
                c("LeftSentinel", 10, 20, 40, 60));
        final QueryTable right = testRefreshingTable(i(3, 5, 7, 9).toTracking(),
                c("USym", "aa", "bb", "aa", "bb"),
                c("Sym", "aa_1", "bb_1", "aa_2", "bb_2"),
                c("RightSentinel", 30, 50, 70, 90));

        final PartitionedTable leftPT = left.partitionBy("USym");
        final PartitionedTable rightPT = right.partitionBy("USym");

        final PartitionedTable.Proxy leftProxy = leftPT.proxy(true, true);
        final PartitionedTable.Proxy rightProxy = rightPT.proxy(true, true);

        final PartitionedTable.Proxy result = leftProxy.join(rightProxy, "Sym", "RightSentinel");

        final Table mergedResult = result.target().merge();
        TableTools.show(mergedResult);

        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        addToTable(left, i(8), c("USym", "bb"), c("Sym", "aa_1"), c("LeftSentinel", 80));

        allowingError(() -> {
            left.notifyListeners(i(8), i(), i());
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
        }, throwables -> {
            // We should deliver a failure to every dependent node
            TestCase.assertTrue(getUpdateErrors().size() > 0);
            final Throwable throwable = throwables.get(0);
            TestCase.assertEquals(IllegalArgumentException.class, throwable.getClass());
            TestCase.assertTrue(throwable.getMessage().contains("has join keys found in multiple constituents"));
            return true;
        });

    }

    public void testDependencies() {
        final QueryTable sourceTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                c("USym", "aa", "bb", "aa", "bb"),
                c("Sentinel", 10, 20, 40, 60));

        final PartitionedTable result = sourceTable.partitionBy("USym");
        final Table aa = result.constituentFor("aa");
        final Table aa2 = aa.update("S2=Sentinel * 2");
        TableTools.show(aa2);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(
                () -> TestCase.assertTrue(aa2.satisfied(LogicalClock.DEFAULT.currentStep())));

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(sourceTable, i(8), c("USym", "bb"), c("Sentinel", 80));
            sourceTable.notifyListeners(i(8), i(), i());
            TestCase.assertFalse(((QueryTable) aa2).satisfied(LogicalClock.DEFAULT.currentStep()));
            // We need to flush one notification: one for the source table because we do not require an intermediate
            // view table in this case
            final boolean flushed = UpdateGraphProcessor.DEFAULT.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);
            TestCase.assertTrue(aa2.satisfied(LogicalClock.DEFAULT.currentStep()));
        });
    }

    public static class PauseHelper {
        private final long start = System.currentTimeMillis();
        private volatile boolean released = false;

        @SuppressWarnings("unused")
        public <T> T pauseValue(T retVal) {
            System.out.println((System.currentTimeMillis() - start) / 1000.0 + ": Reading: " + retVal);

            synchronized (this) {
                while (!released) {
                    try {
                        System.out.println(
                                (System.currentTimeMillis() - start) / 1000.0 + ": Waiting for release of: " + retVal);
                        wait(5000);
                        if (!released) {
                            TestCase.fail("Not released!");
                        }
                        System.out.println((System.currentTimeMillis() - start) / 1000.0 + ": Release of: " + retVal);
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
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 2, 0, 0);

        final QueryTable sourceTable = testRefreshingTable(i(1, 2).toTracking(),
                c("USym", "aa", "bb"),
                c("Sentinel", 10, 20));

        final QueryTable sourceTable2 = testRefreshingTable(i(3, 5).toTracking(),
                c("USym2", "aa", "bb"),
                c("Sentinel2", 30, 50));

        final PartitionedTable result = sourceTable.partitionBy("USym");

        final PauseHelper pauseHelper = new PauseHelper();
        final PauseHelper pauseHelper2 = new PauseHelper();
        QueryScope.addParam("pauseHelper", pauseHelper);
        QueryScope.addParam("pauseHelper2", pauseHelper);

        pauseHelper.release();
        pauseHelper2.release();

        final ExecutionContext executionContext = ExecutionContext.newBuilder()
                .captureQueryScopeVars("pauseHelper2")
                .captureQueryLibrary()
                .captureQueryCompiler()
                .build();
        final PartitionedTable result2 =
                sourceTable2.update("SlowItDown=pauseHelper.pauseValue(k)").partitionBy("USym2")
                        .transform(executionContext, t -> t.update("SlowItDown2=pauseHelper2.pauseValue(2 * k)"));

        // pauseHelper.pause();
        pauseHelper2.pause();

        final PartitionedTable joined = result.partitionedTransform(result2, (l, r) -> {
            System.out.println("Doing naturalJoin");
            return l.naturalJoin(r, "USym=USym2");
        });
        final Table merged = joined.merge();

        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
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

        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();

        pauseHelper2.pause();

        System.out.println("Before second cycle.");
        TableTools.showWithRowSet(sourceTable);
        TableTools.showWithRowSet(sourceTable2);

        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
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

        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();

        TableTools.showWithRowSet(merged);
    }

    public void testCrossDependencies2() {
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 2, 0, 0);

        final QueryTable sourceTable = testRefreshingTable(i(1, 2).toTracking(),
                c("USym", "aa", "bb"),
                c("Sentinel", 10, 20));

        final QueryTable sourceTable2 = testRefreshingTable(i(3, 5, 9).toTracking(),
                c("USym2", "aa", "bb", "dd"),
                c("Sentinel2", 30, 50, 90));

        final PartitionedTable result = sourceTable.partitionBy("USym");

        final PauseHelper pauseHelper = new PauseHelper();
        QueryScope.addParam("pauseHelper", pauseHelper);

        pauseHelper.release();

        final ExecutionContext executionContext = ExecutionContext.newBuilder()
                .captureQueryScopeVars("pauseHelper")
                .captureQueryLibrary()
                .captureQueryCompiler()
                .build();
        final PartitionedTable result2 = sourceTable2.partitionBy("USym2")
                .transform(executionContext, t -> t.update("SlowItDown2=pauseHelper.pauseValue(2 * k)"));

        final PartitionedTable joined = result.partitionedTransform(result2, (l, r) -> {
            System.out.println("Doing naturalJoin");
            return l.naturalJoin(r, "USym=USym2");
        });
        final Table merged = joined.merge();

        pauseHelper.pause();
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
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

        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();

        TableTools.showWithRowSet(merged);
    }

    public void testPartitionedTableScope() {
        testPartitionedTableScope(true);
        testPartitionedTableScope(false);
    }

    private void testPartitionedTableScope(boolean refreshing) {
        final Table table = TableTools.newTable(TableTools.col("Key", "A", "B"), intCol("Value", 1, 2));
        if (refreshing) {
            table.setRefreshing(true);
        }

        final SafeCloseable scopeCloseable = LivenessScopeStack.open();

        final PartitionedTable partitionedTable = table.partitionBy("Key");
        final Table value = partitionedTable.constituentFor("A");
        assertNotNull(value);

        final SingletonLivenessManager manager = new SingletonLivenessManager(partitionedTable);

        UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(scopeCloseable::close);

        if (refreshing) {
            org.junit.Assert.assertTrue(partitionedTable.tryRetainReference());
            org.junit.Assert.assertTrue(value.tryRetainReference());

            partitionedTable.dropReference();
            value.dropReference();
        }

        final SafeCloseable scopeCloseable2 = LivenessScopeStack.open();
        final Table valueAgain = partitionedTable.constituentFor("A");
        assertSame(value, valueAgain);
        UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(scopeCloseable2::close);

        UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(manager::release);

        org.junit.Assert.assertFalse(value.tryRetainReference());
        org.junit.Assert.assertFalse(partitionedTable.tryRetainReference());
    }

    private void testMemoize(Table source, Function<Table, PartitionedTable> op) {
        testMemoize(source, op, op);
    }

    private void testMemoize(Table source, Function<Table, PartitionedTable> op,
            Function<Table, PartitionedTable> op2) {
        final PartitionedTable result = op.apply(source);
        final PartitionedTable result2 = op2.apply(source);
        org.junit.Assert.assertSame(result, result2);
    }

    private void testNoMemoize(Table source, Function<Table, PartitionedTable> op,
            Function<Table, PartitionedTable> op2) {
        final PartitionedTable result = op.apply(source);
        final PartitionedTable result2 = op2.apply(source);
        org.junit.Assert.assertNotSame(result, result2);
    }

    public void testMemoize() {
        final QueryTable sourceTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                c("USym", "aa", "bb", "aa", "bb"),
                c("Sentinel", 10, 20, 40, 60));

        final boolean old = QueryTable.setMemoizeResults(true);
        try {
            testMemoize(sourceTable, t -> t.partitionBy("USym"));
            testMemoize(sourceTable, t -> t.partitionBy("Sentinel"));
            testMemoize(sourceTable, t -> t.partitionBy(true, "USym"));
            testMemoize(sourceTable, t -> t.partitionBy(true, "Sentinel"));
            testMemoize(sourceTable, t -> t.partitionBy(false, "Sentinel"), t -> t.partitionBy("Sentinel"));
            testNoMemoize(sourceTable, t -> t.partitionBy(true, "Sentinel"), t -> t.partitionBy("Sentinel"));
            testNoMemoize(sourceTable, t -> t.partitionBy("USym"), t -> t.partitionBy("Sentinel"));
        } finally {
            QueryTable.setMemoizeResults(old);
        }
    }

    public void testMergeUpdating() {
        final int seed = 0;
        final Random random = new Random(seed);

        final int size = 10_000;
        final ColumnInfo[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "IntCol", "DoubleCol"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd"),
                        new IntGenerator(0, 100),
                        new DoubleGenerator(0, 10)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return table.partitionBy().merge();
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return table.partitionBy("Sym")
                                .sort(List.of(SortColumn.asc(ColumnName.of("Sym"))))
                                .merge();
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return table.partitionBy("Sym", "IntCol")
                                .sort(List.of(
                                        SortColumn.asc(ColumnName.of("Sym")),
                                        SortColumn.asc(ColumnName.of("IntCol"))))
                                .merge();
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return table.partitionBy().merge().flatten().select();
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return table.partitionBy("Sym")
                                .sort(List.of(SortColumn.asc(ColumnName.of("Sym"))))
                                .merge()
                                .flatten()
                                .select();
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return table.partitionBy("Sym", "IntCol")
                                .sort(List.of(
                                        SortColumn.asc(ColumnName.of("Sym")),
                                        SortColumn.asc(ColumnName.of("IntCol"))))
                                .merge()
                                .flatten()
                                .select();
                    }
                },

                new UpdateValidatorNugget(table.partitionBy().merge()),
                new UpdateValidatorNugget(table.partitionBy("Sym").merge()),
                new UpdateValidatorNugget(table.partitionBy("Sym", "IntCol").merge()),
        };

        for (int step = 0; step < 100; ++step) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Seed=" + seed + ", step=" + step + ", size=" + table.size());
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    public void testMergeConstituentChanges() {
        final QueryTable base = (QueryTable) emptyTable(10).update("II=ii");
        base.setRefreshing(true);

        final MutableLong step = new MutableLong(0);
        QueryScope.addParam("step", step);
        ExecutionContext.getContext().getQueryLibrary().importStatic(TableTools.class);

        final Table underlying;
        try (final SafeCloseable ignored = ExecutionContext.makeExecutionContext(false).open()) {
            underlying = base.update(
                    "Constituent=emptyTable(1000 * step.longValue()).update(\"JJ=ii * \" + II + \" * step.longValue()\")");
        }

        final PartitionedTable partitioned = PartitionedTableFactory.of(underlying);
        final Table merged = partitioned.merge();

        final RowSet evenModifies = RowSetFactory.fromKeys(0, 2, 4, 6, 8);
        final RowSet oddModifies = RowSetFactory.fromKeys(1, 3, 5, 7, 9);
        final ModifiedColumnSet modifiedColumnSet = base.getModifiedColumnSetForUpdates();
        modifiedColumnSet.clear();
        modifiedColumnSet.setAll("II");
        while (step.incrementAndGet() <= 100) {
            final boolean evenStep = step.longValue() % 2 == 0;
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                base.notifyListeners(new TableUpdateImpl(
                        RowSetFactory.empty(),
                        RowSetFactory.empty(),
                        evenStep ? evenModifies.copy() : oddModifies.copy(),
                        RowSetShiftData.EMPTY,
                        modifiedColumnSet));
            });

            final Table[] tables = LongStream.range(0, 10).mapToObj((final long II) -> {
                final boolean evenPos = II % 2 == 0;
                if (evenStep == evenPos) {
                    return emptyTable(1000 * step.longValue())
                            .updateView("JJ = ii * " + II + " * step.longValue()");
                } else {
                    return emptyTable(1000 * (step.longValue() - 1))
                            .updateView("JJ = ii * " + II + " * (step.longValue() - 1)");
                }
            }).toArray(Table[]::new);
            final Table matching = TableTools.merge(tables);
            assertTableEquals(matching, merged);
        }
    }

    public void testMergeStaticAndRefreshing() {
        final Table staticTable;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            staticTable = emptyTable(100).update("II=ii");
        }
        final Table refreshingTable = emptyTable(100).update("II=100 + ii");
        refreshingTable.setRefreshing(true);
        final Table mergedTable = PartitionedTableFactory.ofTables(staticTable, refreshingTable).merge();
        assertTableEquals(mergedTable, emptyTable(200).update("II=ii"));
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            mergedTable.getRowSet().writableCast().removeRange(0, 1);
            ((BaseTable) mergedTable).notifyListeners(i(), ir(0, 1), i());
        });
    }

    private EvalNugget newExecutionContextNugget(
            QueryTable table, Function<PartitionedTable.Proxy, PartitionedTable.Proxy> op) {
        return new EvalNugget() {
            @Override
            protected Table e() {
                // note we cannot reuse the execution context and remove the values as the table is built each iteration
                try (final SafeCloseable ignored = ExecutionContext.newBuilder()
                        .captureQueryCompiler()
                        .captureQueryLibrary()
                        .newQueryScope()
                        .build().open()) {

                    ExecutionContext.getContext().getQueryScope().putParam("queryScopeVar", "queryScopeValue");
                    ExecutionContext.getContext().getQueryScope().putParam("queryScopeFilter", 50000);

                    final PartitionedTable.Proxy proxy = table.partitionedAggBy(List.of(), true, null, "intCol")
                            .proxy(false, false);
                    final Table result = op.apply(proxy).target().merge().sort("intCol");

                    ExecutionContext.getContext().getQueryScope().putParam("queryScopeVar", null);
                    ExecutionContext.getContext().getQueryScope().putParam("queryScopeFilter", null);
                    return result;
                }
            }
        };
    }

    public void testExecutionContext() {
        final Random random = new Random(0);

        final int size = 100;

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"intCol", "indices"},
                        new IntGenerator(0, 100000),
                        new SortedLongGenerator(0, Long.MAX_VALUE - 1)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                newExecutionContextNugget(table, src -> src.update("K = queryScopeVar")),
                newExecutionContextNugget(table, src -> src.updateView("K = queryScopeVar")),
                newExecutionContextNugget(table, src -> src.select("K = queryScopeVar", "indices", "intCol")),
                newExecutionContextNugget(table, src -> src.view("K = queryScopeVar", "indices", "intCol")),
                newExecutionContextNugget(table, src -> src.where("intCol > queryScopeFilter")),
                newExecutionContextNugget(table, src -> src.update("X = 0", "Y = X")),
        };

        for (int i = 0; i < 100; i++) {
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    public void testTransformDependencyCorrectness() {
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 2, 0, 0);

        final Table input = emptyTable(2).update("First=ii", "Second=100*ii");
        input.setRefreshing(true);

        final Table filter = emptyTable(2).update("Only=ii");
        filter.setRefreshing(true);
        filter.getRowSet().writableCast().remove(1);

        final PartitionedTable partitioned = input.partitionBy("First");
        final ExecutionContext executionContext = TestExecutionContext.createForUnitTests();
        final PartitionedTable transformed = partitioned.transform(executionContext, tableIn -> {
            final QueryTable tableOut = (QueryTable) tableIn.getSubTable(tableIn.getRowSet());
            tableIn.addUpdateListener(new BaseTable.ListenerImpl("Slow Listener", tableIn, tableOut) {
                @Override
                public void onUpdate(TableUpdate upstream) {
                    try {
                        // This is lame, but a better approach requires very strict notification execution ordering,
                        // and will *break* correct implementations since the requisite ordering to trigger the desired
                        // error case is prevented by correct constituent dependency management.
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                    super.onUpdate(upstream);
                }
            });
            return tableOut;
        });

        final PartitionedTable filtered = PartitionedTableFactory.of(transformed.table().whereIn(filter, "First=Only"),
                transformed.keyColumnNames(), transformed.uniqueKeys(), transformed.constituentColumnName(),
                transformed.constituentDefinition(), transformed.constituentChangesPermitted());
        // If we (incorrectly) deliver PT notifications ahead of constituent notifications, we will cause a
        // notification-on-instantiation-step error for this update.
        final PartitionedTable filteredTransformed = filtered.transform(executionContext,
                t -> t.update("Third=22.2*Second"));

        TestCase.assertEquals(1, filteredTransformed.table().size());

        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        try {
            ((BaseTable) input).notifyListeners(i(), i(), i(1));
            filter.getRowSet().writableCast().insert(1);
            ((BaseTable) filter).notifyListeners(i(1), i(), i());
        } finally {
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
        }

        TestCase.assertEquals(2, filteredTransformed.table().size());
    }
}
