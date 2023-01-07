/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.TstUtils.i;

@Category(OutOfBandTest.class)
public class SparseSelectTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testSparseSelect() {
        final int[] sizes;
        if (SHORT_TESTS) {
            sizes = new int[] {20, 4_000};
        } else {
            sizes = new int[] {1000, 10_000};
        }
        for (final int size : sizes) {
            for (int seed = 0; seed < 1; ++seed) {
                System.out.println(DateTime.now() + ": Size = " + size + ", seed=" + seed);
                try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                    testSparseSelect(size, seed);
                }
            }
        }
    }

    private void testSparseSelect(int size, int seed) {
        final Random random = new Random(seed);
        final ColumnInfo[] columnInfo;

        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sym", "intCol", "doubleCol", "boolCol", "floatCol", "longCol", "charCol",
                                "byteCol", "shortCol", "dateTime"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new BooleanGenerator(0.5, 0.1),
                        new FloatGenerator(-1000.0f, 1000.0f),
                        new LongGenerator(),
                        new CharGenerator('a', 'z'),
                        new ByteGenerator(),
                        new ShortGenerator(),
                        new UnsortedDateTimeGenerator(DateTimeUtils.convertDateTime("2019-01-10T00:00:00 NY"),
                                DateTimeUtils.convertDateTime("2019-01-20T00:00:00 NY"))));

        final Table sortedTable = queryTable.sort("intCol");

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
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
                        return SparseSelect.sparseSelect(queryTable, "dateTime");
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
                        return SparseSelect
                                .sparseSelect(TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect
                                .sparseSelect(TableTools.merge(queryTable, queryTable, queryTable, queryTable));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return SparseSelect
                                .sparseSelect(TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable)
                                        .groupBy("Sym").sort("Sym").ungroup());
                    }
                },
                new QueryTableTestBase.TableComparator(
                        TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable).groupBy("Sym").sort("Sym")
                                .ungroup(),
                        SparseSelect.sparseSelect(TableTools.merge(sortedTable, sortedTable, sortedTable, sortedTable)
                                .groupBy("Sym").sort("Sym").ungroup())),
                new QueryTableTestBase.TableComparator(queryTable, SparseSelect.sparseSelect(queryTable)),
                new QueryTableTestBase.TableComparator(queryTable,
                        SparseSelect.partialSparseSelect(queryTable, Arrays.asList("shortCol", "dateTime"))),
                new QueryTableTestBase.TableComparator(sortedTable, SparseSelect.sparseSelect(sortedTable))
        };

        for (int step = 0; step < 25; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step: " + step);
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testSparseSelectWideIndex() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final int[] intVals = new int[63];
        for (int ii = 0; ii < 63; ii++) {
            builder.appendKey(1L << ii);
            intVals[ii] = ii;
        }
        final QueryTable table = TstUtils.testRefreshingTable(builder.build().toTracking(),
                TableTools.intCol("Value", intVals));
        final Table selected = SparseSelect.sparseSelect(table);
        assertTableEquals(selected, table);
    }

    @Test
    public void testSparseSelectSkipMemoryColumns() {
        final int[] intVals = {1, 2, 3, 4, 5};
        final Table table = TstUtils.testRefreshingTable(RowSetFactory.flat(5).toTracking(),
                TableTools.intCol("Value", intVals))
                .update("V2=Value*2");
        final Table selected = SparseSelect.sparseSelect(table);
        assertTableEquals(table, selected);
        TestCase.assertSame(table.getColumnSource("V2"), selected.getColumnSource("V2"));
        TestCase.assertNotSame(table.getColumnSource("Value"), selected.getColumnSource("Value"));

        final Table tt = TableTools.newTable(TableTools.intCol("Val", intVals)).updateView("V2=Val*2");
        TestCase.assertTrue(tt.getColumnSource("Val") instanceof ArrayBackedColumnSource);
        final Table selected2 = SparseSelect.sparseSelect(tt);
        TestCase.assertSame(tt.getColumnSource("Val"), selected2.getColumnSource("Val"));
        TestCase.assertNotSame(tt.getColumnSource("V2"), selected2.getColumnSource("V2"));
    }

    @Test
    public void testSparseSelectReuse() {

        final QueryTable table = TstUtils.testRefreshingTable(i(1, 1L << 20 + 1).toTracking(),
                TableTools.longCol("Value", 1, 2));


        final Table selected = SparseSelect.sparseSelect(table);
        assertTableEquals(selected, table);
        assertTableEquals(TstUtils.prevTable(selected), TstUtils.prevTable(table));

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2), TableTools.longCol("Value", 3));
            table.notifyListeners(i(2), i(), i());
        });

        assertTableEquals(selected, table);
        assertTableEquals(TstUtils.prevTable(selected), TstUtils.prevTable(table));

        TableTools.show(table);
        TableTools.show(selected);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(1L << 20 + 2), TableTools.longCol("Value", 4));
            table.notifyListeners(i(1L << 20 + 2), i(), i());
        });

        TableTools.show(table);
        TableTools.show(selected);

        assertTableEquals(selected, table);
        assertTableEquals(TstUtils.prevTable(selected), TstUtils.prevTable(table));
    }
}
