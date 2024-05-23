//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Partition;
import io.deephaven.base.FileUtils;
import io.deephaven.base.SleepUtil;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.exceptions.TableInitializationException;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.PartitionedTable.Proxy;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.generator.SortedLongGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.mutable.MutableLong;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static io.deephaven.api.agg.Aggregation.AggLast;
import static io.deephaven.api.agg.Aggregation.AggSum;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static org.assertj.core.api.Assertions.assertThat;

@Category(OutOfBandTest.class)
public class PartitionedTableTest extends RefreshingTableTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        setExpectError(false);
    }

    public void testMergeSimple() {
        final QueryTable queryTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                col("Sym", "aa", "bb", "aa", "bb"),
                col("intCol", 10, 20, 40, 60),
                col("doubleCol", 0.1, 0.2, 0.4, 0.6));

        final Table withK = queryTable.update("K=k");

        final PartitionedTable partitionedTable = withK.partitionBy("Sym");
        final Table merged = partitionedTable.merge();
        final Table mergedByK = merged.sort("K");

        if (printTableUpdates) {
            TableTools.show(withK);
            TableTools.show(mergedByK);
        }

        assertTableEquals(mergedByK, withK);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3, 9), col("Sym", "cc", "cc"), col("intCol", 30, 90), col("doubleCol", 2.3, 2.9));
            queryTable.notifyListeners(i(3, 9), i(), i());
        });

        if (printTableUpdates) {
            TableTools.show(withK);
            TableTools.show(merged);
            TableTools.show(mergedByK);
        }

        assertTableEquals(mergedByK, withK);
    }

    public void testMergePopulate() {
        final QueryTable queryTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                col("Sym", "aa", "bb", "aa", "bb"), col("intCol", 10, 20, 40, 60),
                col("doubleCol", 0.1, 0.2, 0.4, 0.6));

        final Table withK = queryTable.update("K=k");

        final QueryTable keyTable = testTable(col("Sym", "cc", "dd"));
        final PartitionedTable partitionedTable = withK.partitionedAggBy(List.of(), true, keyTable, "Sym");

        final Table merged = partitionedTable.merge();
        final Table mergedByK = merged.sort("K");

        if (printTableUpdates) {
            TableTools.show(withK);
            TableTools.show(mergedByK);
        }

        assertTableEquals(mergedByK, withK);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3, 9), col("Sym", "cc", "cc"), col("intCol", 30, 90), col("doubleCol", 2.3, 2.9));
            queryTable.notifyListeners(i(3, 9), i(), i());
        });

        if (printTableUpdates) {
            TableTools.show(withK);
            TableTools.show(mergedByK);
        }

        assertTableEquals(mergedByK, withK);
    }

    public void testMergeIncremental() {
        for (int seed = 1; seed < 2; ++seed) {
            testMergeIncremental(seed);
        }
    }

    private void testMergeIncremental(int seed) {
        final Random random = new Random(seed);

        final int size = 100;

        final ColumnInfo<?, ?>[] columnInfo;
        final String[] syms = {"aa", "bb", "cc", "dd"};
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>(syms),
                        new IntGenerator(0, 20),
                        new DoubleGenerator(0, 100)));

        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return table.partitionedAggBy(List.of(), true, testTable(col("Sym", syms)), "Sym")
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

        final ColumnInfo<?, ?>[] columnInfo;
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
                withK.partitionedAggBy(List.of(), true, testTable(col("Sym", "aa", "bb", "cc", "dd")), "Sym");
        final PartitionedTable.Proxy leftProxy = leftPT.proxy(false, false);

        final PartitionedTable rightPT =
                rightTable.partitionedAggBy(List.of(), true, testTable(col("Sym", "aa", "bb", "cc", "dd")), "Sym");
        final PartitionedTable.Proxy rightProxy = rightPT.proxy(false, false);

        final Table initialKeys = newTable(col("Sym", "cc", "dd", "aa", "bb"), intCol("intCol", 0, 2, 3, 4));
        final PartitionedTable.Proxy initialKeysProxy = initialKeys.partitionBy("Sym").proxy();

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return table.update("K=Indices")
                                .partitionedAggBy(List.of(), true, testTable(col("Sym", "aa", "bb", "cc", "dd")), "Sym")
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
                new QueryTableTest.TableComparator(
                        withK.aggBy(List.of(AggLast("K"), AggSum("doubleCol")), "Sym", "intCol").sort("Sym", "intCol"),
                        leftProxy.aggBy(List.of(AggLast("Sym", "K"), AggSum("doubleCol")), "intCol").target().merge()
                                .moveColumnsUp("Sym").sort("Sym", "intCol")),
                new QueryTableTest.TableComparator(
                        withK.aggBy(List.of(AggLast("K"), AggSum("doubleCol")), false, initialKeys,
                                ColumnName.from("Sym", "intCol")).sort("Sym", "intCol"),
                        leftProxy.aggBy(List.of(AggLast("K"), AggSum("doubleCol")), false, initialKeys,
                                ColumnName.from("Sym", "intCol")).target().merge().sort("Sym", "intCol")),
                new QueryTableTest.TableComparator(
                        withK.aggBy(List.of(AggLast("K"), AggSum("doubleCol")), false, initialKeys,
                                ColumnName.from("Sym", "intCol")).sort("Sym", "intCol"),
                        leftProxy.aggBy(List.of(AggLast("K"), AggSum("doubleCol")), false, initialKeysProxy,
                                ColumnName.from("Sym", "intCol")).target().merge().sort("Sym", "intCol")),
        };
        for (int i = 0; i < 100; i++) {
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    public void testTransformPartitionedTableThenMerge() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);

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
                                        t -> t.update("K2=Key * 2").update("K3=Key + K2").update("K5 = K3 + K2"), true)
                                .merge().sort("Key");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return partitionedTable
                                .partitionedTransform(partitionedTable, executionContext,
                                        (l, r) -> l.naturalJoin(r.lastBy("Key"), "Key", "Sentinel2=Sentinel"), true)
                                .merge().sort("Key");
                    }
                }
        };

        final int iterations = SHORT_TESTS ? 40 : 100;
        for (int ii = 0; ii < iterations; ++ii) {
            final int iteration = ii + 1;
            updateGraph.runWithinUnitTestCycle(() -> {
                final long baseLocation = iteration * 10L;
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
                col("Sym", "aa", "bb", "aa", "bb"),
                col("intCol", 10, 20, 40, 60),
                col("doubleCol", 0.1, 0.2, 0.4, 0.6));

        queryTable.setAttribute(Table.SORTABLE_COLUMNS_ATTRIBUTE, "bar");

        final PartitionedTable partitionedTable = queryTable.partitionBy("Sym");

        for (Table table : partitionedTable.constituents()) {
            ((QueryTable) table).setAttribute("quux", "baz");
        }

        Table merged = partitionedTable.merge();
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

        final AtomicInteger tableCounter = new AtomicInteger(1);
        final PartitionedTable transformed = partitionedTable.transform(
                t -> t.withAttributes(Map.of("differing", tableCounter.getAndIncrement())));

        // the merged table just takes the set that is consistent
        merged = transformed.merge();
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
                col("USym", "aa", "bb", "aa", "bb"),
                col("Sym", "aa_1", "bb_1", "aa_2", "bb_2"),
                col("LeftSentinel", 10, 20, 40, 60));
        final QueryTable right = testRefreshingTable(i(3, 5, 7, 9).toTracking(),
                col("USym", "aa", "bb", "aa", "bb"),
                col("Sym", "aa_1", "bb_1", "aa_2", "bb_2"),
                col("RightSentinel", 30, 50, 70, 90));

        final PartitionedTable leftPT = left.partitionBy("USym");
        final PartitionedTable rightPT = right.partitionBy("USym");

        final PartitionedTable.Proxy leftProxy = leftPT.proxy(true, true);
        final PartitionedTable.Proxy rightProxy = rightPT.proxy(true, true);

        final PartitionedTable.Proxy result = leftProxy.join(rightProxy, "Sym", "RightSentinel");

        final Table mergedResult = result.target().merge();
        TableTools.show(mergedResult);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests();
        addToTable(left, i(8), col("USym", "bb"), col("Sym", "aa_1"), col("LeftSentinel", 80));

        allowingError(() -> {
            left.notifyListeners(i(8), i(), i());
            updateGraph.completeCycleForUnitTests();
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
                col("USym", "aa", "bb", "aa", "bb"),
                col("Sentinel", 10, 20, 40, 60));

        final PartitionedTable result = sourceTable.partitionBy("USym");
        final Table aa = result.constituentFor("aa");
        final Table aa2 = aa.update("S2=Sentinel * 2");
        TableTools.show(aa2);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> TestCase.assertTrue(aa2.satisfied(updateGraph.clock().currentStep())));

        // We need to flush one notification: one for the source table because we do not require an intermediate
        // view table in this case
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(sourceTable, i(8), col("USym", "bb"), col("Sentinel", 80));
            sourceTable.notifyListeners(i(8), i(), i());
            TestCase.assertFalse(aa2.satisfied(updateGraph.clock().currentStep()));
            // We need to flush one notification: one for the source table because we do not require an intermediate
            // view table in this case
            final boolean flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);
            TestCase.assertTrue(aa2.satisfied(updateGraph.clock().currentStep()));
        });
    }

    public void testTransformDependencies() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final QueryTable sourceTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                col("USym", "aa", "bb", "aa", "bb"),
                col("Sentinel", 10, 20, 40, 60));

        final QueryTable extraTable = testRefreshingTable(i(0).toTracking(),
                col("Value", "0.2"));
        final MutableBoolean extraParentSatisfied = new MutableBoolean(false);
        final NotificationQueue.Dependency extraParentDependency = new NotificationQueue.Dependency() {
            @Override
            public boolean satisfied(long step) {
                return extraParentSatisfied.booleanValue();
            }

            @Override
            public UpdateGraph getUpdateGraph() {
                return updateGraph;
            }

            @Override
            public LogOutput append(LogOutput logOutput) {
                return logOutput.append("extra dependency");
            }
        };
        extraTable.addParentReference(extraParentDependency);

        final PartitionedTable partitioned = sourceTable.partitionBy("USym");
        final PartitionedTable transformed = partitioned.transform(t -> t.join(extraTable), extraTable);

        // We need to flush one notification: one for the source table because we do not require an intermediate
        // view table in this case
        updateGraph.runWithinUnitTestCycle(() -> {
            // Add "dd" to source
            addToTable(sourceTable, i(8), col("USym", "dd"), col("Sentinel", 80));
            sourceTable.notifyListeners(i(8), i(), i());
            TestCase.assertTrue(updateGraph.flushOneNotificationForUnitTests());

            // PartitionBy has processed "dd"
            TestCase.assertTrue(partitioned.table().satisfied(updateGraph.clock().currentStep()));
            TestCase.assertNotNull(partitioned.constituentFor("dd"));

            // Transform has not processed "dd" yet
            TestCase.assertFalse(transformed.table().satisfied(updateGraph.clock().currentStep()));
            TestCase.assertNull(transformed.constituentFor("dd"));

            // Flush the notification for transform's internal copy() of partitioned.table()
            TestCase.assertTrue(updateGraph.flushOneNotificationForUnitTests());

            // Add a row to extra
            addToTable(extraTable, i(1), col("Value", "0.3"));
            extraTable.notifyListeners(i(1), i(), i());
            TestCase.assertFalse(updateGraph.flushOneNotificationForUnitTests(true)); // Fail to update anything

            extraParentSatisfied.setTrue(); // Allow updates to propagate
            updateGraph.flushAllNormalNotificationsForUnitTests();

            TestCase.assertTrue(transformed.table().satisfied(updateGraph.clock().currentStep()));
            final Table transformedDD = transformed.constituentFor("dd");
            TestCase.assertTrue(transformedDD.satisfied(updateGraph.clock().currentStep()));
            TestCase.assertEquals(2, transformedDD.size());
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
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 2, 0, 0);

        final QueryTable sourceTable = testRefreshingTable(i(1, 2).toTracking(),
                col("USym", "aa", "bb"),
                col("Sentinel", 10, 20));

        final QueryTable sourceTable2 = testRefreshingTable(i(3, 5).toTracking(),
                col("USym2", "aa", "bb"),
                col("Sentinel2", 30, 50));

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
                sourceTable2.update("SlowItDown=pauseHelper.pauseValue(k)").partitionBy("USym2").transform(
                        executionContext, t -> t.withAttributes(Map.of(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE, "true"))
                                .update("SlowItDown2=pauseHelper2.pauseValue(2 * k)"),
                        true);

        // pauseHelper.pause();
        pauseHelper2.pause();

        final PartitionedTable joined = result.partitionedTransform(result2, (l, r) -> {
            System.out.println("Doing naturalJoin");
            return l.naturalJoin(r, "USym=USym2");
        });
        final Table merged = joined.merge();

        updateGraph.startCycleForUnitTests();
        addToTable(sourceTable, i(3), col("USym", "cc"), col("Sentinel", 30));
        addToTable(sourceTable2, i(7, 9), col("USym2", "cc", "dd"), col("Sentinel2", 70, 90));
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

        updateGraph.completeCycleForUnitTests();

        pauseHelper2.pause();

        System.out.println("Before second cycle.");
        TableTools.showWithRowSet(sourceTable);
        TableTools.showWithRowSet(sourceTable2);

        updateGraph.startCycleForUnitTests();
        addToTable(sourceTable, i(4, 5), col("USym", "cc", "dd"), col("Sentinel", 40, 50));
        addToTable(sourceTable2, i(8, 10), col("USym2", "cc", "dd"), col("Sentinel2", 80, 100));
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

        updateGraph.completeCycleForUnitTests();

        TableTools.showWithRowSet(merged);
    }

    public void testCrossDependencies2() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 2, 0, 0);

        final QueryTable sourceTable = testRefreshingTable(i(1, 2).toTracking(),
                col("USym", "aa", "bb"),
                col("Sentinel", 10, 20));

        final QueryTable sourceTable2 = testRefreshingTable(i(3, 5, 9).toTracking(),
                col("USym2", "aa", "bb", "dd"),
                col("Sentinel2", 30, 50, 90));

        final PartitionedTable result = sourceTable.partitionBy("USym");

        final PauseHelper pauseHelper = new PauseHelper();
        QueryScope.addParam("pauseHelper", pauseHelper);

        pauseHelper.release();

        final ExecutionContext executionContext = ExecutionContext.newBuilder()
                .captureQueryScopeVars("pauseHelper")
                .captureQueryLibrary()
                .captureQueryCompiler()
                .build();
        final PartitionedTable result2 = sourceTable2.partitionBy("USym2").transform(executionContext,
                t -> t.withAttributes(Map.of(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE, "true"))
                        .update("SlowItDown2=pauseHelper.pauseValue(2 * k)"),
                true);

        final PartitionedTable joined = result.partitionedTransform(result2, (l, r) -> {
            System.out.println("Doing naturalJoin");
            return l.naturalJoin(r, "USym=USym2");
        });
        final Table merged = joined.merge();

        pauseHelper.pause();
        updateGraph.startCycleForUnitTests();
        addToTable(sourceTable, i(5), col("USym", "dd"), col("Sentinel", 50));
        addToTable(sourceTable2, i(10), col("USym2", "dd"), col("Sentinel2", 100));
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

        updateGraph.completeCycleForUnitTests();

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

        ExecutionContext.getContext().getUpdateGraph().exclusiveLock().doLocked(scopeCloseable::close);

        if (refreshing) {
            org.junit.Assert.assertTrue(partitionedTable.tryRetainReference());
            org.junit.Assert.assertTrue(value.tryRetainReference());

            partitionedTable.dropReference();
            value.dropReference();
        }

        final SafeCloseable scopeCloseable2 = LivenessScopeStack.open();
        final Table valueAgain = partitionedTable.constituentFor("A");
        assertSame(value, valueAgain);
        ExecutionContext.getContext().getUpdateGraph().exclusiveLock().doLocked(scopeCloseable2::close);

        ExecutionContext.getContext().getUpdateGraph().exclusiveLock().doLocked(manager::release);

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
                col("USym", "aa", "bb", "aa", "bb"),
                col("Sentinel", 10, 20, 40, 60));

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

        final int size = 8_000;
        final ColumnInfo<?, ?>[] columnInfo;
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
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        while (step.incrementAndGet() <= 100) {
            final boolean evenStep = step.longValue() % 2 == 0;
            updateGraph.runWithinUnitTestCycle(() -> {
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
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            mergedTable.getRowSet().writableCast().removeRange(0, 1);
            ((BaseTable<?>) mergedTable).notifyListeners(i(), RowSetFactory.fromRange(0, 1), i());
        });
    }

    public void testSnapshotWhen() {
        final Random random = new Random(0);
        final Table testTable = newTable(
                getRandomIntCol("a", 100, random),
                getRandomIntCol("b", 100, random),
                getRandomIntCol("c", 100, random),
                getRandomIntCol("d", 100, random),
                getRandomIntCol("e", 100, random));
        final PartitionedTable partitionedTable = testTable.partitionBy("c");
        final Proxy selfPtProxy = partitionedTable.proxy();
        final Table triggerTable = timeTable("PT00:00:01");
        final Proxy ptProxy = selfPtProxy.snapshotWhen(triggerTable);
        assertThat(ptProxy.target().constituentDefinition().numColumns()).isEqualTo(6);
        for (Table constituent : ptProxy.target().constituents()) {
            System.out.println(constituent);
        }
    }

    private EvalNugget newExecutionContextNugget(
            String name,
            QueryTable table,
            Function<PartitionedTable.Proxy, PartitionedTable.Proxy> op) {
        return new EvalNugget() {
            @Override
            protected Table e() {
                // note we cannot reuse the execution context and remove the values as the table is built each iteration
                try (final SafeCloseable ignored = ExecutionContext.newBuilder()
                        .newQueryScope()
                        .captureQueryCompiler()
                        .captureQueryLibrary()
                        .build().open()) {

                    ExecutionContext.getContext().getQueryScope().putParam("queryScopeVar", "queryScopeValue");
                    ExecutionContext.getContext().getQueryScope().putParam("queryScopeFilter", 50000);

                    final PartitionedTable.Proxy proxy = table
                            .partitionedAggBy(List.of(Partition.of(name + "Constituent")), true, null, "intCol")
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
                newExecutionContextNugget("UpdateNugget1", table, src -> src.update("K = queryScopeVar")),
                newExecutionContextNugget("UpdateViewNugget", table, src -> src.updateView("K = queryScopeVar")),
                newExecutionContextNugget("SelectNugget", table,
                        src -> src.select("K = queryScopeVar", "indices", "intCol")),
                newExecutionContextNugget("ViewNugget", table,
                        src -> src.view("K = queryScopeVar", "indices", "intCol")),
                newExecutionContextNugget("WhereNugget", table, src -> src.where("intCol > queryScopeFilter")),
                newExecutionContextNugget("UpdateNugget2", table, src -> src.update("X = 0", "Y = X")),
        };

        for (int i = 0; i < 100; i++) {
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    public void testTransformDependencyCorrectness() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 2, 0, 0);

        final Table input = emptyTable(2).update("First=ii", "Second=100*ii");
        input.setRefreshing(true);

        final Table filter = emptyTable(2).update("Only=ii");
        filter.setRefreshing(true);
        filter.getRowSet().writableCast().remove(1);

        final PartitionedTable partitioned = input.partitionBy("First");
        final ExecutionContext executionContext = ExecutionContext.newBuilder()
                .emptyQueryScope()
                .newQueryLibrary()
                .captureQueryCompiler()
                .build();
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
        }, true);

        final PartitionedTable filtered = PartitionedTableFactory.of(transformed.table().whereIn(filter, "First=Only"),
                transformed.keyColumnNames(), transformed.uniqueKeys(), transformed.constituentColumnName(),
                transformed.constituentDefinition(), transformed.constituentChangesPermitted());
        // If we (incorrectly) deliver PT notifications ahead of constituent notifications, we will cause a
        // notification-on-instantiation-step error for this update.
        final PartitionedTable filteredTransformed = filtered.transform(executionContext,
                t -> t.update("Third=22.2*Second"), true);

        TestCase.assertEquals(1, filteredTransformed.table().size());

        updateGraph.startCycleForUnitTests();
        try {
            ((BaseTable<?>) input).notifyListeners(i(), i(), i(1));
            filter.getRowSet().writableCast().insert(1);
            ((BaseTable<?>) filter).notifyListeners(i(1), i(), i());
        } finally {
            updateGraph.completeCycleForUnitTests();
        }

        TestCase.assertEquals(2, filteredTransformed.table().size());
    }

    public void testTransformStaticToRefreshing() {
        final Random random = new Random(0);
        final Table staticInput = newTable(
                getRandomIntCol("a", 100, random),
                getRandomIntCol("b", 100, random),
                getRandomIntCol("c", 100, random),
                getRandomIntCol("d", 100, random),
                getRandomIntCol("e", 100, random));
        final Table refreshingInput = staticInput.withAttributes(Map.of("refreshingClone", true));
        refreshingInput.setRefreshing(true);

        final PartitionedTable partitionedTable = staticInput.partitionBy("c");
        assertFalse(partitionedTable.table().isRefreshing());

        final PartitionedTable.Proxy staticProxy = partitionedTable.proxy();

        final PartitionedTable.Proxy joinedProxy = staticProxy.join(refreshingInput, "c", "c2=c");
        assertTrue(joinedProxy.target().table().isRefreshing());
        assertEquals(staticProxy.target().constituents().length, joinedProxy.target().constituents().length);

        final PartitionedTable partitionedTransformResult = partitionedTable.partitionedTransform(partitionedTable,
                null, (t, u) -> t.join(refreshingInput, "c", "c2=c"), true);
        assertTrue(partitionedTransformResult.table().isRefreshing());
        assertEquals(partitionedTable.constituents().length, partitionedTransformResult.constituents().length);

        try {
            partitionedTable.transform(t -> t.join(refreshingInput, "c", "c2=c"));
            TestCase.fail("Expected exception");
        } catch (TableInitializationException expected) {
            Assert.eqTrue(expected.getCause().getClass() == IllegalStateException.class,
                    "expected.getCause().getClass() instanceof IllegalStateException");
        }

        try {
            partitionedTable.partitionedTransform(partitionedTable, (t, u) -> t.join(refreshingInput, "c", "c2=c"));
            TestCase.fail("Expected exception");
        } catch (TableInitializationException expected) {
            Assert.eqTrue(expected.getCause().getClass() == IllegalStateException.class,
                    "expected.getCause().getClass() instanceof IllegalStateException");
        }
    }

    public void testPartitionedTableSort() throws IOException {
        final File tmpDir = Files.createTempDirectory("PartitionedTableTest-").toFile();
        try {
            final ParquetInstructions instructions = ParquetInstructions.builder().useDictionary("I", true).build();
            Table a = emptyTable(200).update("I = `` + (50 + (ii % 100))", "K = ii");
            Table b = emptyTable(200).update("I = `` + (ii % 100)", "K = ii");
            ParquetTools.writeTable(a, new java.io.File(tmpDir + "/Partition=p0/data.parquet"), instructions);
            ParquetTools.writeTable(b, new java.io.File(tmpDir + "/Partition=p1/data.parquet"), instructions);
            a = a.updateView("Partition = `p0`").moveColumnsUp("Partition");
            b = b.updateView("Partition = `p1`").moveColumnsUp("Partition");

            final Table fromDisk = ParquetTools.readTable(tmpDir);

            // Assert non-partitioned table sorts.
            final Table diskOuterSort = fromDisk.sort("I");
            final Table exOuterSort = TableTools.merge(a, b).sort("I");
            assertTableEquals(exOuterSort, diskOuterSort);

            // Assert partitioned table sorts.
            final Table diskInnerSort = fromDisk.partitionBy("Partition").proxy().sort("I").target().merge();
            final Table exInnerSort = TableTools.merge(a.sort("I"), b.sort("I"));
            assertTableEquals(exInnerSort, diskInnerSort);
        } finally {
            FileUtils.deleteRecursively(tmpDir);
        }
    }
}
