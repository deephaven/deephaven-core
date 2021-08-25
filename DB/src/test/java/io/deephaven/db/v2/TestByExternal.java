/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.TableDiff;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.liveness.LivenessScope;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.util.liveness.SingletonLivenessManager;
import io.deephaven.db.v2.by.SortedFirstBy;
import io.deephaven.db.v2.select.MatchFilter;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Assert;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.v2.TstUtils.getTable;
import static io.deephaven.db.v2.TstUtils.i;
import static io.deephaven.db.v2.by.ComboAggregateFactory.Agg;
import static io.deephaven.db.v2.by.ComboAggregateFactory.AggCombo;

@Category(OutOfBandTest.class)
public class TestByExternal extends QueryTableTestBase {

    class TableMapNugget implements EvalNuggetInterface {
        Table originalTable;
        private final String[] groupByColumns;
        private final ColumnSource[] groupByColumnSources;
        TableMap splitTable;

        TableMapNugget(Table originalTable, String... groupByColumns) {
            this.originalTable = originalTable;
            this.groupByColumns = groupByColumns;
            splitTable = originalTable.byExternal(groupByColumns);
            this.groupByColumnSources = new ColumnSource[groupByColumns.length];
            for (int ii = 0; ii < groupByColumns.length; ++ii) {
                groupByColumnSources[ii] = originalTable.getColumnSource(groupByColumns[ii]);
            }
        }

        @Override
        public void validate(String msg) {
            // get all the keys from the original table
            final HashSet<Object> keys = new HashSet<>();

            for (final Index.Iterator it = originalTable.getIndex().iterator(); it.hasNext();) {
                final long next = it.nextLong();
                if (groupByColumnSources.length == 1) {
                    keys.add(groupByColumnSources[0].get(next));
                } else {
                    final Object[] key = new Object[groupByColumnSources.length];
                    for (int ii = 0; ii < key.length; ++ii) {
                        key[ii] = groupByColumnSources[ii].get(next);
                    }
                    keys.add(new SmartKey(key));
                }
            }

            for (Object key : keys) {
                final Table tableFromMap = splitTable.get(key);

                final Table whereTable;
                if (groupByColumnSources.length == 1) {
                    whereTable = originalTable.where(new MatchFilter(groupByColumns[0], key));
                } else {
                    final MatchFilter[] filters = new MatchFilter[groupByColumnSources.length];
                    for (int ii = 0; ii < groupByColumns.length; ++ii) {
                        filters[ii] =
                            new MatchFilter(groupByColumns[ii], ((SmartKey) key).values_[ii]);
                    }
                    whereTable = originalTable.where(filters);
                }

                if (tableFromMap == null) {
                    System.out.println("Missing key: " + key);
                } else {
                    System.out.println("Checking key: " + key + ", size: " + tableFromMap.size()
                        + " vs. " + whereTable.size());
                }
                final String diff = diff(tableFromMap, whereTable, 10,
                    EnumSet.of(TableDiff.DiffItems.DoublesExact));
                Assert.assertEquals(msg, "", diff);
            }
        }

        @Override
        public void show() {
            showWithIndex(originalTable);

        }
    }

    public void testByExternal() {
        final Random random = new Random(0);
        final int size = 50;

        final TstUtils.ColumnInfo[] columnInfo = new TstUtils.ColumnInfo[3];
        columnInfo[0] =
            new TstUtils.ColumnInfo<>(new TstUtils.SetGenerator<>("a", "b", "c", "d", "e"), "Sym",
                TstUtils.ColumnInfo.ColAttributes.Immutable);
        columnInfo[1] = new TstUtils.ColumnInfo<>(new TstUtils.IntGenerator(10, 20), "intCol",
            TstUtils.ColumnInfo.ColAttributes.Immutable);
        columnInfo[2] =
            new TstUtils.ColumnInfo<>(new TstUtils.SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable queryTable = getTable(size, random, columnInfo);
        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                // new TableMapNugget(queryTable, "Sym"),
                // new TableMapNugget(queryTable, "Sym", "intCol"),
                new TableMapNugget(queryTable, "Sym", "intCol", "doubleCol")
        };

        final int steps = 50;
        for (int i = 0; i < steps; i++) {
            simulateShiftAwareStep("step == " + i, size, random, queryTable, columnInfo, en);
        }
    }

    public void testErrorPropagation() {
        try (final ErrorExpectation ee = new ErrorExpectation()) {
            final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6),
                col("Key", "A", "B", "A"), intCol("Int", 2, 4, 6));

            final TableMap byKey = table.byExternal("Key");

            final Table tableA = byKey.get("A");
            final Table tableB = byKey.get("B");

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 8));
                table.notifyListeners(i(8), i(), i());
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 10));
            });

            final ErrorListener listenerA = new ErrorListener((DynamicTable) tableA);
            final ErrorListener listenerB = new ErrorListener((DynamicTable) tableB);
            ((DynamicTable) tableA).listenForUpdates(listenerA);
            ((DynamicTable) tableB).listenForUpdates(listenerB);

            assertNull(listenerA.originalException);
            assertNull(listenerB.originalException);

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.removeRows(table, i(8));
                table.notifyListeners(i(), i(8), i());
            });

            assertNotNull(listenerA.originalException);
            assertNotNull(listenerB.originalException);
        }
    }

    public void testNewKeysAfterResultReleased() {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), col("Key", "A", "B", "A"),
            intCol("Int", 2, 4, 6));

        final LivenessScope subTablesScope = new LivenessScope();

        try (final SafeCloseable ignored1 = LivenessScopeStack.open(subTablesScope, true)) {

            final TableMap byKey;
            final Table tableA;
            final Table tableB;

            try (final SafeCloseable ignored2 = LivenessScopeStack.open()) {
                byKey = table.byExternal("Key");
                try (
                    final SafeCloseable ignored3 = LivenessScopeStack.open(subTablesScope, false)) {
                    tableA = byKey.get("A");
                    tableB = byKey.get("B");
                }
            }

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 8));
                table.notifyListeners(i(8), i(), i());
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(9), col("Key", "C"), intCol("Int", 10)); // Added row,
                                                                                      // wants to
                                                                                      // make new
                                                                                      // state
                table.notifyListeners(i(9), i(), i());
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertNull(byKey.get("C"));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 11)); // Modified
                                                                                      // row, wants
                                                                                      // to move
                                                                                      // from
                                                                                      // existent
                                                                                      // state to
                                                                                      // nonexistent
                                                                                      // state
                table.notifyListeners(i(), i(), i(8));
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertNull(byKey.get("C"));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 12)); // Modified
                                                                                      // row,
                                                                                      // staying in
                                                                                      // nonexistent
                                                                                      // state
                table.notifyListeners(i(), i(), i(8));
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertNull(byKey.get("C"));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 13)); // Modified
                                                                                      // row, wants
                                                                                      // to move
                                                                                      // from
                                                                                      // nonexistent
                                                                                      // state to
                                                                                      // existent
                                                                                      // state
                table.notifyListeners(i(), i(), i(8));
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertNull(byKey.get("C"));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 14)); // Modified
                                                                                      // row,
                                                                                      // staying in
                                                                                      // existent
                                                                                      // state
                table.notifyListeners(i(), i(), i(8));
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertNull(byKey.get("C"));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.removeRows(table, i(9)); // Removed row from a nonexistent state
                table.notifyListeners(i(), i(9), i());
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertNull(byKey.get("C"));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.removeRows(table, i(8)); // Removed row from an existent state
                table.notifyListeners(i(), i(8), i());
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertNull(byKey.get("C"));
        }
    }

    public void testNewKeysBeforeResultReleased() {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6), col("Key", "A", "B", "A"),
            intCol("Int", 2, 4, 6));

        try (final SafeCloseable ignored1 = LivenessScopeStack.open()) {

            final TableMap byKey = table.byExternal("Key");
            final Table tableA = byKey.get("A");
            final Table tableB = byKey.get("B");

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertNull(byKey.get("C"));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 8));
                table.notifyListeners(i(8), i(), i());
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertNull(byKey.get("C"));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(9), col("Key", "C"), intCol("Int", 10)); // Added row,
                                                                                      // makes new
                                                                                      // state
                table.notifyListeners(i(9), i(), i());
            });

            final Table tableC = byKey.get("C");
            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertEquals("", TableTools.diff(tableC, table.where("Key=`C`"), 10));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 11)); // Modified
                                                                                      // row, wants
                                                                                      // to move
                                                                                      // from
                                                                                      // original
                                                                                      // state to
                                                                                      // new state
                table.notifyListeners(i(), i(), i(8));
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertEquals("", TableTools.diff(tableC, table.where("Key=`C`"), 10));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "C"), intCol("Int", 12)); // Modified
                                                                                      // row,
                                                                                      // staying in
                                                                                      // new state
                table.notifyListeners(i(), i(), i(8));
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertEquals("", TableTools.diff(tableC, table.where("Key=`C`"), 10));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 13)); // Modified
                                                                                      // row, wants
                                                                                      // to move
                                                                                      // from new
                                                                                      // state to
                                                                                      // original
                                                                                      // state
                table.notifyListeners(i(), i(), i(8));
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertEquals("", TableTools.diff(tableC, table.where("Key=`C`"), 10));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(table, i(8), col("Key", "B"), intCol("Int", 14)); // Modified
                                                                                      // row,
                                                                                      // staying in
                                                                                      // original
                                                                                      // state
                table.notifyListeners(i(), i(), i(8));
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertEquals("", TableTools.diff(tableC, table.where("Key=`C`"), 10));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.removeRows(table, i(9)); // Removed row from a new state
                table.notifyListeners(i(), i(9), i());
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertEquals("", TableTools.diff(tableC, table.where("Key=`C`"), 10));

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                TstUtils.removeRows(table, i(8)); // Removed row from an original state
                table.notifyListeners(i(), i(8), i());
            });

            assertEquals("", TableTools.diff(tableA, table.where("Key=`A`"), 10));
            assertEquals("", TableTools.diff(tableB, table.where("Key=`B`"), 10));
            assertEquals("", TableTools.diff(tableC, table.where("Key=`C`"), 10));
        }
    }

    public static class SleepHelper {
        long start = System.currentTimeMillis();

        @SuppressWarnings("unused")
        public <T> T sleepValue(long duration, T retVal) {
            System.out
                .println((System.currentTimeMillis() - start) / 1000.0 + ": Reading: " + retVal);
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

        final QueryTable rawTable = TstUtils.testRefreshingTable(i(2, 4, 6),
            col("Key", "A", "B", "A"), intCol("Int", 2, 4, 6), intCol("I2", 1, 2, 3));

        QueryScope.addParam("sleepHelper", new SleepHelper());

        // make it slow to read key
        final Table table = rawTable.updateView("Key = sleepHelper.sleepValue(0, Key)", "K2=1",
            "Int=sleepHelper.sleepValue(250, Int)");

        final SingletonLivenessManager mapManager;

        final Table rollup;

        try (final SafeCloseable ignored1 = LivenessScopeStack.open()) {
            rollup = table.rollup(AggCombo(Agg(new SortedFirstBy("Int"), "Int")), "Key", "K2");
            mapManager = new SingletonLivenessManager(rollup);
        }

        final MutableLong start = new MutableLong();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(rawTable, i(8), col("Key", "C"), intCol("Int", 8), intCol("I2", 5));
            rawTable.notifyListeners(i(8), i(), i());
            start.setValue(System.currentTimeMillis());
        });
        System.out.println("Completion took: " + (System.currentTimeMillis() - start.getValue()));

        final MutableObject<Future<?>> mutableFuture = new MutableObject<>();

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(rawTable, i(10, 11, 12), col("Key", "C", "D", "E"),
                intCol("Int", 8, 9, 10), intCol("I2", 6, 7, 8));
            rawTable.notifyListeners(i(10, 11, 12), i(), i());

            mutableFuture.setValue(pool.submit(() -> {
                try {
                    Thread.sleep(1100);
                } catch (InterruptedException ignored) {
                }
                mapManager.release();
                System.out.println("Releasing map!");
            }));

            start.setValue(System.currentTimeMillis());
        });
        System.out.println("Completion took: " + (System.currentTimeMillis() - start.getValue()));

        try {
            mutableFuture.getValue().get();
        } catch (InterruptedException | ExecutionException e) {
            TestCase.fail(e.getMessage());
        }

        pool.shutdownNow();
    }

    public void testPopulateKeysStatic() {
        final Table table = emptyTable(1).update("USym=`AAPL`", "Value=1");
        final TableMap map = table.byExternal("USym");
        map.populateKeys("SPY");
        System.out.println(Arrays.toString(map.getKeySet()));
        assertEquals(map.getKeySet(), new String[] {"AAPL", "SPY"});
        assertFalse(((TableMapImpl) map).isRefreshing());
    }

    public void testPopulateKeysRefreshing() {
        final Table table = emptyTable(1).update("USym=`AAPL`", "Value=1");
        ((BaseTable) table).setRefreshing(true);
        final TableMap map = table.byExternal("USym");
        map.populateKeys("SPY");
        System.out.println(Arrays.toString(map.getKeySet()));
        assertEquals(map.getKeySet(), new String[] {"AAPL", "SPY"});
        assertTrue(((TableMapImpl) map).isRefreshing());
    }

    public void testByExternalWithShifts() {
        for (int seed = 0; seed < 100; ++seed) {
            System.out.println("Seed = " + seed);
            testByExternalWithShifts(seed);
        }
    }

    private void testByExternalWithShifts(int seed) {
        final Random random = new Random(seed);
        final int size = 10;

        final TstUtils.ColumnInfo[] columnInfo = new TstUtils.ColumnInfo[3];
        columnInfo[0] =
            new TstUtils.ColumnInfo<>(new TstUtils.SetGenerator<>("a", "b", "c", "d", "e"), "Sym",
                TstUtils.ColumnInfo.ColAttributes.Immutable);
        columnInfo[1] = new TstUtils.ColumnInfo<>(new TstUtils.IntGenerator(10, 20), "intCol",
            TstUtils.ColumnInfo.ColAttributes.Immutable);
        columnInfo[2] =
            new TstUtils.ColumnInfo<>(new TstUtils.SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable queryTable = getTable(size, random, columnInfo);
        final Table simpleTable =
            TableTools.newTable(TableTools.col("Sym", "a"), TableTools.intCol("intCol", 30),
                TableTools.doubleCol("doubleCol", 40.1)).updateView("K=-2L");
        final Table source = TableTools.merge(simpleTable, queryTable.updateView("K=k")).flatten();

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.Sorted.from(() -> LiveTableMonitor.DEFAULT.sharedLock()
                    .computeLocked(() -> source.byExternal("Sym").merge()), "Sym"),
                EvalNugget.Sorted.from(
                    () -> LiveTableMonitor.DEFAULT.sharedLock()
                        .computeLocked(() -> source.where("Sym=`a`").byExternal("Sym").merge()),
                    "Sym"),
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
