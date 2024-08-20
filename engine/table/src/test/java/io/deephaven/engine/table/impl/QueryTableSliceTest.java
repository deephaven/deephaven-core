//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.function.CharConsumer;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.generator.SortedLongGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;

import io.deephaven.test.types.OutOfBandTest;
import java.util.Random;

import org.junit.Assert;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.charCol;
import static io.deephaven.engine.util.TableTools.emptyTable;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;

@Category(OutOfBandTest.class)
public class QueryTableSliceTest extends QueryTableTestBase {
    public void testSliceIncremental() {
        final int[] sizes = {1, 10, 100};
        for (int size : sizes) {
            testSliceIncremental("size == " + size, size);
        }
    }

    private void testSliceIncremental(final String ctxt, final int size) {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol", "Indices"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new SortedLongGenerator(0, Long.MAX_VALUE - 1)));
        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> queryTable.head(0)),
                EvalNugget.from(() -> queryTable.update("x = Indices").head(0)),
                EvalNugget.from(() -> queryTable.updateView("x = Indices").head(0)),
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").head(0);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").head(0);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("Sym").update("x=intCol+1").head(0);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("intCol").update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.head(0).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").head(0).update("x=sum(intCol)").head(0);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").head(0).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.head(1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.update("x = Indices").head(1);
                    }
                },
                EvalNugget.from(() -> queryTable.updateView("x = Indices").head(1)),
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").head(1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").head(1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("Sym").update("x=intCol+1").head(1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("intCol").update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.head(1).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").head(1).update("x=sum(intCol)").head(1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").head(1).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.head(10);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.update("x = Indices").head(10);
                    }
                },
                EvalNugget.from(() -> queryTable.updateView("x = Indices").head(10)),
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").head(10);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").head(10);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("Sym").update("x=intCol+1").head(10);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("intCol").update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.head(10).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").head(10).update("x=sum(intCol)").head(10);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").head(10).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.tail(0);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.update("x = Indices").tail(0);
                    }
                },
                EvalNugget.from(() -> queryTable.updateView("x = Indices").tail(0)),
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").tail(0);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym").tail(0);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("Sym").update("x=intCol+1").tail(0);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("intCol").update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.tail(0).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").tail(0).update("x=sum(intCol)").tail(0);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").tail(0).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.tail(1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.tail(1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.update("x = Indices").tail(1);
                    }
                },
                EvalNugget.from(() -> queryTable.updateView("x = Indices").tail(1)),
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").tail(1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").tail(1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("Sym").update("x=intCol+1").tail(1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("intCol").update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.tail(1).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").tail(1).update("x=sum(intCol)").tail(1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").tail(1).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.tail(10);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.update("x = Indices").tail(10);
                    }
                },
                EvalNugget.from(() -> queryTable.updateView("x = Indices").tail(10)),
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").tail(10);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").tail(10);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("Sym").update("x=intCol+1").tail(10);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("intCol").update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.tail(10).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").sort("Sym").tail(10).update("x=sum(intCol)").tail(10);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").tail(10).update("x=intCol+1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.tail(0);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.slice(5, 10);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.slice(-10, -5);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.slice(0, -5);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.slice(10, -5);
                    }
                },
        };
        final int steps = 8;
        for (int i = 0; i < steps; i++) {
            if (printTableUpdates) {
                System.out.println("\n == Simple Step i = " + i);
                TableTools.showWithRowSet(queryTable);
            }
            simulateShiftAwareStep(ctxt + " step == " + i, size, random, queryTable, columnInfo, en);
        }
    }

    public void testGrowthAppendUpdatePattern() {
        final long steps = 4096;

        for (int j = 1; j < 100; j += 7) {
            final QueryTable upTable = getTable(true, 0, new Random(0), new ColumnInfo[0]);
            upTable.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
            upTable.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
            final QueryTable queryTable = (QueryTable) upTable.updateView("I=i", "II=ii");
            final EvalNugget[] en = {
                    EvalNugget.from(() -> queryTable.slice(10, 15)),
                    EvalNugget.from(() -> queryTable.slice(10, -15)),
                    EvalNugget.from(() -> queryTable.slice(-35, 0)),
                    EvalNugget.from(() -> queryTable.slice(-15, 10))
            };

            for (int i = 0; i < steps; ++i) {
                final long ii = i;
                final long jj = j;
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> {
                    // Appending the rows at the end
                    RowSet added1 = RowSetFactory.fromRange(ii * jj, (ii + 1) * jj - 1);
                    upTable.getRowSet().writableCast().insert(added1);
                    TableUpdate update =
                            new TableUpdateImpl(added1, RowSetFactory.empty(),
                                    RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
                    upTable.notifyListeners(update);
                });

                TstUtils.validate("", en);
            }
        }
    }

    public void testGrowthPrependUpdatePattern() {
        final int numRowsToAdd = 10;
        final int tableSize = 50;
        final int steps = (tableSize / numRowsToAdd);

        final QueryTable upTable = getTable(true, 0, new Random(0), new ColumnInfo[0]);
        upTable.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
        final QueryTable queryTable = (QueryTable) upTable.updateView("K=k");
        final EvalNugget[] en = {
                EvalNugget.from(() -> queryTable.slice(10, 15)),
                EvalNugget.from(() -> queryTable.slice(10, -15)),
                EvalNugget.from(() -> queryTable.slice(-35, 0)),
                EvalNugget.from(() -> queryTable.slice(-15, 10))
        };

        for (int i = steps; i > 0; --i) {
            final long ii = i;
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            // Prepending 10 rows at time, starting from (40, 49), (30, 39)...
            updateGraph.runWithinUnitTestCycle(() -> {
                RowSet added1 = RowSetFactory.fromRange((ii - 1) * numRowsToAdd, ((ii) * numRowsToAdd) - 1);
                upTable.getRowSet().writableCast().insert(added1);
                TableUpdate update =
                        new TableUpdateImpl(added1, RowSetFactory.empty(),
                                RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
                upTable.notifyListeners(update);
            });

            TstUtils.validate("", en);
        }
    }


    public void testShrinkageUpdatePattern() {
        final int numRowsToDelete = 10;
        final int tableSize = 50;
        final int steps = (tableSize / numRowsToDelete);
        final QueryTable upTable = TstUtils.testRefreshingTable(RowSetFactory.fromRange(0, tableSize - 1).toTracking());
        final QueryTable queryTable = (QueryTable) upTable.updateView("K=k");
        final EvalNugget[] en = {
                EvalNugget.from(() -> queryTable.slice(10, 15)),
                EvalNugget.from(() -> queryTable.slice(10, -15)),
                EvalNugget.from(() -> queryTable.slice(-35, 0)),
                EvalNugget.from(() -> queryTable.slice(-15, 10))
        };

        for (int i = 0; i < steps; ++i) {
            final long ii = i;
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                RowSet removed1 = RowSetFactory.fromRange(ii * numRowsToDelete, ((ii + 1) * numRowsToDelete) - 1);
                upTable.getRowSet().writableCast().remove(removed1);
                TableUpdate update =
                        new TableUpdateImpl(RowSetFactory.empty(), removed1,
                                RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
                upTable.notifyListeners(update);
            });

            TstUtils.validate("", en);
        }
    }

    public void testLongTail() {
        final Table bigTable = emptyTable(2 * (long) (Integer.MAX_VALUE)).updateView("I=i", "II=ii");
        final Table tailed = bigTable.tail(1);
        assertEquals(2L * Integer.MAX_VALUE - 1, tailed.getColumnSource("II").get(tailed.getRowSet().firstRowKey()));
    }

    public void testZeroHead() {
        final QueryTable table = TstUtils.testRefreshingTable(
                RowSetFactory.fromRange(10, 35).toTracking(),
                TableTools.charCol("letter", "abcdefghijklmnopqrstuvwxyz".toCharArray()));
        final Table noRows = table.head(0);
        assertEquals(0, noRows.size());
        assertFalse(noRows.isRefreshing());
        final Table oneRow = table.head(1);
        assertEquals(1, oneRow.size());
        assertTrue(oneRow.isRefreshing());
    }

    public void testSlice() {
        final QueryTable table = TstUtils.testRefreshingTable(
                RowSetFactory.fromRange(10, 35).toTracking(),
                TableTools.charCol("letter", "abcdefghijklmnopqrstuvwxyz".toCharArray()));

        doSliceTest(table, "abcdefghij", 0, 10);
        doSliceTest(table, "cdefghij", 2, 10);
        doSliceTest(table, "yz", -2, 0);
        doSliceTest(table, "xy", -3, -1);
        doSliceTest(table, "xyz", -3, 0);
        doSliceTest(table, "abc", 0, -23);
        doSliceTest(table, "klmnopqrstuvwxyz", 10, 27);
        doSliceTest(table, "klmnopqrstuvwxy", 10, 25);
        doSliceTest(table, "klmnopqrstuvwxy", 10, -1);
        doSliceTest(table, "", 10, -18);
        doSliceTest(table, "abcdefghijklmnopqrstuvwxy", 0, -1);
        doSliceTest(table, "bcdefghijklmnopqrstuvwxy", 1, -1);
        doSliceTest(table, "", 2, 2);
        doSliceTest(table, "c", 2, 3);
        doSliceTest(table, "wxy", -4, 25);
        doSliceTest(table, "", -4, 20);
    }

    private void doSliceTest(QueryTable table, String expected, int firstPositionInclusive, int lastPositionExclusive) {
        final StringBuilder chars = new StringBuilder();
        table.slice(firstPositionInclusive, lastPositionExclusive).characterColumnIterator("letter")
                .forEachRemaining((CharConsumer) chars::append);
        final String result = chars.toString();
        assertEquals(expected, result);
    }

    public void testSlicePct() {
        final QueryTable table = TstUtils.testRefreshingTable(
                RowSetFactory.fromRange(10, 19).toTracking(),
                TableTools.charCol("letter", "9876543210".toCharArray()));

        doSlicePctTest(table, "9876543210", 0, 1);
        doSlicePctTest(table, "98765432", 0, 0.8);
        doSlicePctTest(table, "9876543", 0, 0.75);
        doSlicePctTest(table, "9876543", 0, 0.7);
        doSlicePctTest(table, "876543", 0.1, 0.7);
        doSlicePctTest(table, "876543", 0.15, 0.7);
        doSlicePctTest(table, "76543", 0.2, 0.7);

        // Non-overlapping percentage ranges should give non-overlapping results and combined should be equal to the
        // complete table
        doSlicePctTest(table, "98", 0.0, 0.25);
        doSlicePctTest(table, "765", 0.25, 0.5);
        doSlicePctTest(table, "43", 0.5, 0.75);
        doSlicePctTest(table, "210", 0.75, 1);
    }

    private void doSlicePctTest(QueryTable table, String expected, double startPercentInclusive,
            final double endPercentExclusive) {
        final StringBuilder chars = new StringBuilder();
        table.slicePct(startPercentInclusive, endPercentExclusive).characterColumnIterator("letter")
                .forEachRemaining((CharConsumer) chars::append);
        final String result = chars.toString();
        assertEquals(expected, result);
    }

    public void testHeadTailPct() {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", 'a', 'b', 'c'));

        assertTableEquals(table.headPct(0.5),
                TstUtils.testRefreshingTable(i(2, 4).toTracking(), col("x", 1, 2), col("y", 'a', 'b')));
        assertTableEquals(table.tailPct(0.5),
                TstUtils.testRefreshingTable(i(4, 6).toTracking(), col("x", 2, 3), col("y", 'b', 'c')));
        assertTableEquals(table.headPct(0.1),
                TstUtils.testRefreshingTable(i(2).toTracking(), col("x", 1), col("y", 'a')));
        assertTableEquals(table.tailPct(0.1),
                TstUtils.testRefreshingTable(i(6).toTracking(), col("x", 3), col("y", 'c')));
        assertTableEquals(table.headPct(0),
                TstUtils.testRefreshingTable(i().toTracking(), intCol("x"), charCol("y")));
        assertTableEquals(table.tailPct(0),
                TstUtils.testRefreshingTable(i().toTracking(), intCol("x"), charCol("y")));
        assertTableEquals(table.headPct(1), table);
        assertTableEquals(table.tailPct(1), table);

        // Test for invalid parameters (negative or >1)
        try {
            table.headPct(-0.5);
            Assert.fail("Exception expected for invalid arguments");
        } catch (IllegalArgumentException expected) {
        }
        try {
            table.tailPct(-0.5);
            Assert.fail("Exception expected for invalid arguments");
        } catch (IllegalArgumentException expected) {
        }
        try {
            table.headPct(1.5);
            Assert.fail("Exception expected for invalid arguments");
        } catch (IllegalArgumentException expected) {
        }
        try {
            table.tailPct(1.5);
            Assert.fail("Exception expected for invalid arguments");
        } catch (IllegalArgumentException expected) {
        }
    }

    public void testHeadTailPctIncremental() {
        final int[] sizes = {1, 10, 100};
        for (int size : sizes) {
            testHeadTailPctIncremental("size == " + size, size);
        }
    }

    private void testHeadTailPctIncremental(final String ctxt, final int size) {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));
        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return queryTable.headPct(0.5);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.headPct(0.1);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.tailPct(0.5);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.tailPct(0.1);
                    }
                },
        };
        final int steps = 8;
        for (int i = 0; i < steps; i++) {
            simulateShiftAwareStep(ctxt + " step == " + i, size, random, queryTable, columnInfo, en);
        }
    }
}
