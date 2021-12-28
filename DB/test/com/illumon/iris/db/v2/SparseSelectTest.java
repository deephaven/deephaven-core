package com.illumon.iris.db.v2;

import com.illumon.iris.db.tables.Table;
import com.illumon.iris.db.tables.live.LiveTableMonitor;
import com.illumon.iris.db.tables.utils.DBDateTime;
import com.illumon.iris.db.tables.utils.DBTimeUtils;
import com.illumon.iris.db.tables.utils.TableTools;
import com.illumon.iris.db.util.liveness.LivenessScopeStack;
import com.illumon.iris.db.v2.sources.ArrayBackedColumnSource;
import com.illumon.iris.db.v2.utils.Index;
import com.illumon.util.SafeCloseable;

import java.util.Arrays;
import java.util.Random;

import static com.illumon.iris.db.tables.utils.TableTools.intCol;
import static com.illumon.iris.db.tables.utils.TableTools.longCol;
import static com.illumon.iris.db.v2.TstUtils.*;
import static com.illumon.iris.db.v2.TstUtils.prevTable;

public class SparseSelectTest extends QueryTableTestBase {
    public void testSparseSelect() {
        int size = 1000;
        for (int seed = 0; seed < 1; ++seed) {
            System.out.println(DBDateTime.now() + ": Size = " + size + ", seed=" + seed);
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                testSparseSelect(size, seed);
            }
        }
        size = 10000;
        for (int seed = 0; seed < 1; ++seed) {
            System.out.println(DBDateTime.now() + ": Size = " + size + ", seed=" + seed);
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                testSparseSelect(size, seed);
            }
        }
    }

    private void testSparseSelect(int size, int seed) {
        final Random random = new Random(seed);
        final TstUtils.ColumnInfo [] columnInfo;

        final QueryTable queryTable = getTable(size, random, columnInfo = initColumnInfos(new String[]{"Sym", "intCol", "doubleCol", "boolCol", "floatCol", "longCol", "charCol", "byteCol", "shortCol", "dbDateTime"},
                new TstUtils.SetGenerator<>("a", "b","c","d", "e"),
                new TstUtils.IntGenerator(10, 100),
                new TstUtils.SetGenerator<>(10.1, 20.1, 30.1),
                new TstUtils.BooleanGenerator(0.5, 0.1),
                new TstUtils.FloatGenerator(-1000.0f, 1000.0f),
                new TstUtils.LongGenerator(),
                new TstUtils.CharGenerator('a', 'z'),
                new TstUtils.ByteGenerator(),
                new TstUtils.ShortGenerator(),
                new TstUtils.UnsortedDateTimeGenerator(DBTimeUtils.convertDateTime("2019-01-10T00:00:00 NY"), DBTimeUtils.convertDateTime("2019-01-20T00:00:00 NY"))));

        final Table sortedTable = queryTable.sort("intCol");

        final EvalNuggetInterface en[] = new EvalNuggetInterface[]{
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(queryTable, "Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.partialSparseSelect(queryTable, "Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(queryTable, "boolCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(queryTable, "dbDateTime");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(queryTable);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(sortedTable);
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(TableTools.merge(queryTable, queryTable, queryTable, queryTable));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect.sparseSelect(TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable).by("Sym").sort("Sym").ungroup());
                    }
                },
                new QueryTableTestBase.TableComparator(TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable).by("Sym").sort("Sym").ungroup(), SparseSelect.sparseSelect(TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable).by("Sym").sort("Sym").ungroup())),
                new QueryTableTestBase.TableComparator(queryTable, SparseSelect.sparseSelect(queryTable)),
                new QueryTableTestBase.TableComparator(queryTable, SparseSelect.partialSparseSelect(queryTable, Arrays.asList("shortCol", "dbDateTime"))),
                new QueryTableTestBase.TableComparator(sortedTable, SparseSelect.sparseSelect(sortedTable))
        };

        for (int step = 0; step < 25; step++) {
            if (printTableUpdates) {
                System.out.println("Step: " + step);
            }
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    public void testSparseSelectWideIndex() {
        final Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        final int [] intVals = new int[63];
        for (int ii = 0; ii < 63; ii++) {
            builder.appendKey(1L << ii);
            intVals[ii] = ii;
        }
        final QueryTable table = TstUtils.testRefreshingTable(builder.getIndex(), intCol("Value", intVals));
        final Table selected = SparseSelect.sparseSelect(table);
        final String diff = TableTools.diff(selected, table, 10);
        assertEquals("", diff);
    }

    public void testSparseSelectSkipMemoryColumns() {
        final int [] intVals = {1, 2, 3, 4, 5};
        final Table table = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(5), intCol("Value", intVals)).update("V2=Value*2");
        final Table selected = SparseSelect.sparseSelect(table);
        assertTableEquals(table, selected);
        assertSame(table.getColumnSource("V2"), selected.getColumnSource("V2"));
        assertNotSame(table.getColumnSource("Value"), selected.getColumnSource("Value"));

        final Table tt = TableTools.newTable(intCol("Val", intVals)).updateView("V2=Val*2");
        assertTrue(tt.getColumnSource("Val") instanceof ArrayBackedColumnSource);
        final Table selected2 = SparseSelect.sparseSelect(tt);
        assertSame(tt.getColumnSource("Val"), selected2.getColumnSource("Val"));
        assertNotSame(tt.getColumnSource("V2"), selected2.getColumnSource("V2"));
    }

    public void testSparseSelectReuse() {

        final QueryTable table = TstUtils.testRefreshingTable(i(1, 1L<<20+1), longCol("Value", 1, 2));


        final Table selected = SparseSelect.sparseSelect(table);
        final String diff = TableTools.diff(selected, table, 10);
        assertEquals("", diff);

        final String diffPrev = TableTools.diff(prevTable(selected), prevTable(table), 10);
        assertEquals("", diffPrev);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2), longCol("Value", 3));
            table.notifyListeners(i(2), i(), i());
        });

        final String diff2 = TableTools.diff(selected, table, 10);
        assertEquals("", diff2);

        final String diffPrev2 = TableTools.diff(prevTable(selected), prevTable(table), 10);
        assertEquals("", diffPrev2);

        TableTools.show(table);
        TableTools.show(selected);


        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(1L<<20 + 2), longCol("Value", 4));
            table.notifyListeners(i(1L<<20 + 2), i(), i());
        });

        TableTools.show(table);
        TableTools.show(selected);

        final String diff3 = TableTools.diff(selected, table, 10);
        assertEquals("", diff3);

        final String diffPrev3 = TableTools.diff(prevTable(selected), prevTable(table), 10);
        assertEquals("", diffPrev3);
    }
}
