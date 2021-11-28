package io.deephaven.engine.table.impl;

import io.deephaven.base.Procedure;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;

import io.deephaven.test.types.OutOfBandTest;
import java.io.IOException;
import java.util.Random;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.util.TableTools.diff;
import static io.deephaven.engine.util.TableTools.emptyTable;
import static io.deephaven.engine.util.TableTools.showWithRowSet;
import static io.deephaven.engine.table.impl.TstUtils.*;
import static io.deephaven.engine.table.impl.TstUtils.initColumnInfos;

@Category(OutOfBandTest.class)
public class QueryTableSliceTest extends QueryTableTestBase {
    public void testSliceIncremental() throws IOException {
        final int[] sizes = {1, 10, 100};
        for (int size : sizes) {
            testSliceIncremental("size == " + size, size);
        }
    }

    private void testSliceIncremental(final String ctxt, final int size) throws IOException {
        final Random random = new Random(0);
        final TstUtils.ColumnInfo columnInfo[];
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol", "Indices"},
                        new TstUtils.SetGenerator<>("a", "b", "c", "d"),
                        new TstUtils.IntGenerator(10, 100),
                        new TstUtils.SetGenerator<>(10.1, 20.1, 30.1),
                        new TstUtils.SortedLongGenerator(0, Long.MAX_VALUE - 1)));
        final EvalNugget en[] = new EvalNugget[] {
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

    public void testTailWithGrowth() {
        final long steps = 4096;

        for (int j = 1; j < 100; j += 7) {
            final QueryTable upTable = getTable(true, 0, new Random(0), new ColumnInfo[0]);
            final QueryTable queryTable = (QueryTable) upTable.updateView("I=i", "II=ii");
            final EvalNugget[] en = {
                    EvalNugget.from(() -> queryTable.slice(-35, 0))
            };

            for (int i = 0; i < steps; ++i) {
                final int ii = i;
                final int jj = j;
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                    RowSet added = RowSetFactory.fromRange(ii * jj, (ii + 1) * jj - 1);
                    upTable.getRowSet().writableCast().insert(added);
                    TableUpdate update =
                            new TableUpdateImpl(added, RowSetFactory.empty(),
                                    RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
                    upTable.notifyListeners(update);
                });

                TstUtils.validate("", en);
            }
        }
    }

    public void testLongTail() {
        final Table bigTable = emptyTable(2 * (long) (Integer.MAX_VALUE)).updateView("I=i", "II=ii");
        final Table tailed = bigTable.tail(1);
        assertEquals(2L * Integer.MAX_VALUE - 1, tailed.getColumn("II").get(0));
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
    }

    private void doSliceTest(QueryTable table, String expected, int firstPositionInclusive, int lastPositionExclusive) {
        final StringBuilder chars = new StringBuilder();
        table.slice(firstPositionInclusive, lastPositionExclusive).characterColumnIterator("letter")
                .forEachRemaining((Procedure.UnaryChar) chars::append);
        final String result = chars.toString();
        assertEquals(expected, result);
    }

    public void testHeadTailPct() {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                c("x", 1, 2, 3), c("y", 'a', 'b', 'c'));

        assertEquals("", diff(table.headPct(0.5),
                TstUtils.testRefreshingTable(i(2, 4).toTracking(), c("x", 1, 2), c("y", 'a', 'b')), 10));
        assertEquals("", diff(table.tailPct(0.5),
                TstUtils.testRefreshingTable(i(4, 6).toTracking(), c("x", 2, 3), c("y", 'b', 'c')), 10));
        assertEquals("", diff(table.headPct(0.1),
                TstUtils.testRefreshingTable(i(2).toTracking(), c("x", 1), c("y", 'a')), 10));
        assertEquals("", diff(table.tailPct(0.1),
                TstUtils.testRefreshingTable(i(6).toTracking(), c("x", 3), c("y", 'c')), 10));

    }

    public void testHeadTailPctIncremental() throws IOException {
        final int[] sizes = {1, 10, 100};
        for (int size : sizes) {
            testHeadTailPctIncremental("size == " + size, size);
        }
    }

    private void testHeadTailPctIncremental(final String ctxt, final int size) throws IOException {
        final Random random = new Random(0);
        final ColumnInfo[] columnInfo;
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
