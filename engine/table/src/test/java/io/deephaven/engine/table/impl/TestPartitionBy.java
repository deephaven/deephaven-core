//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.tuple.ArrayTuple;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableLong;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Assert;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.experimental.categories.Category;

import static io.deephaven.api.agg.Aggregation.AggSortedFirst;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

@Category(OutOfBandTest.class)
public class TestPartitionBy extends QueryTableTestBase {

    private static class PartitionedTableNugget implements EvalNuggetInterface {
        Table originalTable;
        private final String[] groupByColumns;
        private final ColumnSource<?>[] groupByColumnSources;
        PartitionedTable splitTable;

        PartitionedTableNugget(Table originalTable, String... groupByColumns) {
            this.originalTable = originalTable;
            this.groupByColumns = groupByColumns;
            splitTable = originalTable.partitionBy(groupByColumns);
            this.groupByColumnSources = new ColumnSource[groupByColumns.length];
            for (int ii = 0; ii < groupByColumns.length; ++ii) {
                groupByColumnSources[ii] = originalTable.getColumnSource(groupByColumns[ii]);
            }
        }

        @Override
        public void validate(String msg) {
            // get all the keys from the original table
            final Map<ArrayTuple, Object[]> tupleToKey = new HashMap<>();

            for (final RowSet.Iterator it = originalTable.getRowSet().iterator(); it.hasNext();) {
                final long next = it.nextLong();
                final Object[] key = new Object[groupByColumnSources.length];
                for (int ii = 0; ii < key.length; ++ii) {
                    key[ii] = groupByColumnSources[ii].get(next);
                }
                tupleToKey.put(new ArrayTuple(key), key);
            }

            for (Object[] key : tupleToKey.values()) {
                final Table constituent = splitTable.constituentFor(key);

                final Table whereTable;
                if (groupByColumnSources.length == 1) {
                    whereTable = originalTable.where(new MatchFilter(MatchType.Regular, groupByColumns[0], key));
                } else {
                    final MatchFilter[] filters = new MatchFilter[groupByColumnSources.length];
                    for (int ii = 0; ii < groupByColumns.length; ++ii) {
                        filters[ii] = new MatchFilter(MatchType.Regular, groupByColumns[ii], key[ii]);
                    }
                    whereTable = originalTable.where(Filter.and(filters));
                }

                if (constituent == null) {
                    System.out.println("Missing key: " + Arrays.toString(key));
                } else {
                    System.out.println("Checking key: " + Arrays.toString(key)
                            + ", size: " + constituent.size()
                            + " vs. " + whereTable.size());
                }
                final String diff = diff(constituent, whereTable, 10, EnumSet.of(TableDiff.DiffItems.DoublesExact));
                Assert.assertEquals(msg, "", diff);
            }
        }

        @Override
        public void show() {
            TableTools.showWithRowSet(originalTable);

        }
    }

    public void testPartitionBy() {
        final Random random = new Random(0);
        final int size = 50;

        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo[3];
        columnInfo[0] = new ColumnInfo<>(new SetGenerator<>("a", "b", "c", "d", "e"), "Sym",
                ColumnInfo.ColAttributes.Immutable);
        columnInfo[1] = new ColumnInfo<>(new IntGenerator(10, 20), "intCol",
                ColumnInfo.ColAttributes.Immutable);
        columnInfo[2] = new ColumnInfo<>(new SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable queryTable = getTable(size, random, columnInfo);
        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                // new PartitionedTableNugget(queryTable, "Sym"),
                // new PartitionedTableNugget(queryTable, "Sym", "intCol"),
                new PartitionedTableNugget(queryTable, "Sym", "intCol", "doubleCol")
        };

        final int steps = 50;
        for (int i = 0; i < steps; i++) {
            simulateShiftAwareStep("step == " + i, size, random, queryTable, columnInfo, en);
        }
    }

    public void testErrorPropagation() {
        try (final ErrorExpectation ignored = new ErrorExpectation()) {
            final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                    col("Key", "A", "B", "A"), intCol("Int", 2, 4, 6));

            final PartitionedTable byKey = table.partitionBy("Key");

            final Table tableA = byKey.constituentFor("A");
            final Table tableB = byKey.constituentFor("B");

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 8));
                table.notifyListeners(i(8), i(), i());
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));

            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 10));
            });

            final ErrorListener listenerA = new ErrorListener(tableA);
            final ErrorListener listenerB = new ErrorListener(tableB);
            tableA.addUpdateListener(listenerA);
            tableB.addUpdateListener(listenerB);

            assertNull(listenerA.originalException());
            assertNull(listenerB.originalException());

            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.removeRows(table, i(8));
                table.notifyListeners(i(), i(8), i());
            });

            assertNotNull(listenerA.originalException());
            assertNotNull(listenerB.originalException());
        }
    }

    public void testNewKeysAfterResultReleased() {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("Key", "A", "B", "A"), intCol("Int", 2, 4, 6));

        final LivenessScope subTablesScope = new LivenessScope();

        try (final SafeCloseable ignored1 = LivenessScopeStack.open(subTablesScope, true)) {

            final PartitionedTable byKey;
            final Table tableA;
            final Table tableB;

            final LivenessScope subTableManager = new LivenessScope();
            try (final SafeCloseable ignored2 = LivenessScopeStack.open()) {
                byKey = table.partitionBy("Key");
                tableA = byKey.constituentFor("A");
                tableB = byKey.constituentFor("B");
                assertNotNull(tableA);
                assertNotNull(tableB);
                subTableManager.manage(tableA);
                subTableManager.manage(tableB);
            }

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 8));
                table.notifyListeners(i(8), i(), i());
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));

            // Added row, wants to make new
            // state
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(9), col("Key", "C"), intCol("Int", 10)); // Added row, wants to make new
                                                                                      // state
                table.notifyListeners(i(9), i(), i());
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            expectLivenessException(() -> byKey.constituentFor("C"));

            // Modified row, wants to move from existent state to nonexistent state
            updateGraph.runWithinUnitTestCycle(() -> {
                // Modified row, wants to move from existent state to nonexistent state
                TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 11));
                table.notifyListeners(i(), i(), i(8));
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            expectLivenessException(() -> byKey.constituentFor("C"));

            // Modified row, staying in nonexistent state
            updateGraph.runWithinUnitTestCycle(() -> {
                // Modified row, staying in nonexistent state
                TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 12));
                table.notifyListeners(i(), i(), i(8));
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            expectLivenessException(() -> byKey.constituentFor("C"));

            // Modified row, wants to move from nonexistent state to existent state
            updateGraph.runWithinUnitTestCycle(() -> {
                // Modified row, wants to move from nonexistent state to existent state
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 13));
                table.notifyListeners(i(), i(), i(8));
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            expectLivenessException(() -> byKey.constituentFor("C"));

            // Modified row, staying in existent state
            updateGraph.runWithinUnitTestCycle(() -> {
                // Modified row, staying in existent state
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 14));
                table.notifyListeners(i(), i(), i(8));
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            expectLivenessException(() -> byKey.constituentFor("C"));

            // Removed row from a nonexistent state
            updateGraph.runWithinUnitTestCycle(() -> {
                // Removed row from a nonexistent state
                TstUtils.removeRows(table, i(9));
                table.notifyListeners(i(), i(9), i());
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            expectLivenessException(() -> byKey.constituentFor("C"));

            // Removed row from an existent state
            updateGraph.runWithinUnitTestCycle(() -> {
                // Removed row from an existent state
                TstUtils.removeRows(table, i(8));
                table.notifyListeners(i(), i(8), i());
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            expectLivenessException(() -> byKey.constituentFor("C"));
        }
    }

    public void testNewKeysBeforeResultReleased() {
        final QueryTable table =
                TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                        col("Key", "A", "B", "A"), intCol("Int", 2, 4, 6));

        try (final SafeCloseable ignored1 = LivenessScopeStack.open()) {

            final PartitionedTable byKey = table.partitionBy("Key");
            final Table tableA = byKey.constituentFor("A");
            final Table tableB = byKey.constituentFor("B");

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            assertNull(byKey.constituentFor("C"));

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 8));
                table.notifyListeners(i(8), i(), i());
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            assertNull(byKey.constituentFor("C"));

            // Added row, makes new state
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(9), col("Key", "C"), intCol("Int", 10)); // Added row, makes new state
                table.notifyListeners(i(9), i(), i());
            });

            final Table tableC = byKey.constituentFor("C");
            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            assertTableEquals(tableC, table.where("Key=`C`"));

            updateGraph.runWithinUnitTestCycle(() -> {
                // Modified row, wants to move from original state to new state
                TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 11));
                table.notifyListeners(i(), i(), i(8));
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            assertTableEquals(tableC, table.where("Key=`C`"));

            updateGraph.runWithinUnitTestCycle(() -> {
                // Modified row, staying in new state
                TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 12));
                table.notifyListeners(i(), i(), i(8));
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            assertTableEquals(tableC, table.where("Key=`C`"));

            updateGraph.runWithinUnitTestCycle(() -> {
                // Modified row, wants to move from new state to original state
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 13));
                table.notifyListeners(i(), i(), i(8));
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            assertTableEquals(tableC, table.where("Key=`C`"));

            updateGraph.runWithinUnitTestCycle(() -> {
                // Modified row, staying in original state
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 14));
                table.notifyListeners(i(), i(), i(8));
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            assertTableEquals(tableC, table.where("Key=`C`"));

            updateGraph.runWithinUnitTestCycle(() -> {
                // Removed row from a new state
                TstUtils.removeRows(table, i(9));
                table.notifyListeners(i(), i(9), i());
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            assertTableEquals(tableC, table.where("Key=`C`"));

            updateGraph.runWithinUnitTestCycle(() -> {
                // Removed row from an original state
                TstUtils.removeRows(table, i(8));
                table.notifyListeners(i(), i(8), i());
            });

            assertTableEquals(tableA, table.where("Key=`A`"));
            assertTableEquals(tableB, table.where("Key=`B`"));
            assertTableEquals(tableC, table.where("Key=`C`"));
        }
    }

    public static class SleepHelper {
        final long start = System.currentTimeMillis();

        @SuppressWarnings("unused")
        public <T> T sleepValue(long duration, T retVal) {
            System.out.println((System.currentTimeMillis() - start) / 1000.0 + ": Reading: " + retVal);
            try {
                Thread.sleep(duration);
            } catch (InterruptedException ignored) {
            }
            return retVal;
        }
    }

    public void testReleaseRaceRollup() {
        setExpectError(false);
        final ExecutorService pool = Executors.newFixedThreadPool(1);

        final QueryTable rawTable = TstUtils.testRefreshingTable(
                i(2, 4, 6).toTracking(),
                col("Key", "A", "B", "A"),
                intCol("Int", 2, 4, 6),
                intCol("I2", 1, 2, 3));

        QueryScope.addParam("sleepHelper", new SleepHelper());

        // Make it slow to read Int
        final Table table = rawTable.updateView(
                "Key = sleepHelper.sleepValue(0, Key)",
                "K2=1",
                "Int=sleepHelper.sleepValue(250, Int)");

        final SingletonLivenessManager rollupManager;

        final RollupTable rollup;

        try (final SafeCloseable ignored1 = LivenessScopeStack.open()) {
            rollup = table.rollup(List.of(AggSortedFirst("Int", "Int")), "Key", "K2");
            rollupManager = new SingletonLivenessManager(rollup);
        }

        final MutableLong start = new MutableLong();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(rawTable,
                    i(8),
                    col("Key", "C"),
                    intCol("Int", 8),
                    intCol("I2", 5));
            rawTable.notifyListeners(i(8), i(), i());
            start.set(System.currentTimeMillis());
        });
        System.out.println("Completion took: " + (System.currentTimeMillis() - start.get()));

        final MutableObject<Future<?>> mutableFuture = new MutableObject<>();

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(rawTable,
                    i(10, 11, 12),
                    col("Key", "C", "D", "E"),
                    intCol("Int", 8, 9, 10),
                    intCol("I2", 6, 7, 8));
            rawTable.notifyListeners(i(10, 11, 12), i(), i());

            mutableFuture.setValue(pool.submit(() -> {
                try {
                    Thread.sleep(1100);
                } catch (InterruptedException ignored) {
                }
                System.out.println("Releasing rollup!");
                rollupManager.release();
            }));

            start.set(System.currentTimeMillis());
        });
        System.out.println("Completion took: " + (System.currentTimeMillis() - start.get()));

        try {
            mutableFuture.getValue().get();
        } catch (InterruptedException | ExecutionException e) {
            TestCase.fail(e.getMessage());
        }

        pool.shutdownNow();
    }

    public void testPopulateKeysStatic() {
        testPopulateKeys(false);
    }

    public void testPopulateKeysRefreshing() {
        testPopulateKeys(true);
    }

    private void testPopulateKeys(final boolean refreshing) {
        final Table table = emptyTable(1).update("USym=`AAPL`", "Value=1");
        if (refreshing) {
            table.setRefreshing(true);
        }
        final PartitionedTable pt = table.partitionedAggBy(List.of(), true, testTable(col("USym", "SPY")), "USym");
        final String keyColumnName = pt.keyColumnNames().stream().findFirst().get();
        final String[] keys = (String[]) DataAccessHelpers.getColumn(pt.table(), keyColumnName).getDirect();
        System.out.println(Arrays.toString(keys));
        assertEquals(keys, new String[] {"SPY", "AAPL"});
        assertEquals(pt.table().isRefreshing(), refreshing);
    }

    public void testPartitionByWithShifts() {
        for (int seed = 0; seed < 100; ++seed) {
            System.out.println("Seed = " + seed);
            testPartitionByWithShifts(seed);
        }
    }

    private void testPartitionByWithShifts(int seed) {
        final Random random = new Random(seed);
        final int size = 10;

        final ColumnInfo<?, ?>[] columnInfo = new ColumnInfo[3];
        columnInfo[0] = new ColumnInfo<>(new SetGenerator<>("a", "b", "c", "d", "e"), "Sym",
                ColumnInfo.ColAttributes.Immutable);
        columnInfo[1] = new ColumnInfo<>(new IntGenerator(10, 20), "intCol",
                ColumnInfo.ColAttributes.Immutable);
        columnInfo[2] = new ColumnInfo<>(new SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable queryTable = getTable(size, random, columnInfo);
        final Table simpleTable = TableTools.newTable(TableTools.col("Sym", "a"), TableTools.intCol("intCol", 30),
                TableTools.doubleCol("doubleCol", 40.1)).updateView("K=-2L");
        final Table source = TableTools.merge(simpleTable, queryTable.updateView("K=k")).flatten();

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.Sorted.from(() -> ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(
                        () -> source.partitionBy("Sym").merge()), "Sym"),
                EvalNugget.Sorted.from(() -> ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(
                        () -> source.where("Sym=`a`").partitionBy("Sym").merge()), "Sym"),
        };

        final int steps = 50;
        for (int step = 0; step < steps; step++) {
            if (printTableUpdates) {
                System.out.println("Step = " + step);
            }
            simulateShiftAwareStep("step == " + step, size, random, queryTable, columnInfo, en);
        }
    }
}
