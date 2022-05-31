package io.deephaven.engine.table.impl.verify;

import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.*;
import io.deephaven.test.junit4.EngineCleanup;
import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;

import java.util.Optional;
import java.util.Random;

import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.table.impl.TstUtils.*;

public class TestTableAssertions {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testStatic() {
        final Table test = TableTools.newTable(stringCol("Plant", "Apple", "Banana", "Carrot", "Daffodil"),
                intCol("Int", 9, 3, 2, 1),
                doubleCol("D1", QueryConstants.NULL_DOUBLE, Math.E, Math.PI, Double.NEGATIVE_INFINITY));

        TestCase.assertSame(test, TableAssertions.assertSorted("test", test, "Plant", SortingOrder.Ascending));
        TestCase.assertSame(test, TableAssertions.assertSorted(test, "Int", SortingOrder.Descending));
        try {
            TableAssertions.assertSorted("test", test, "D1", SortingOrder.Ascending);
            TestCase.fail("Table is not actually sorted by D1");
        } catch (SortedAssertionFailure saf) {
            TestCase.assertEquals(
                    "Table violates sorted assertion, table description=test, column=D1, Ascending, 3.141592653589793 is out of order with respect to -Infinity!",
                    saf.getMessage());
        }

        TestCase.assertEquals(SortingOrder.Ascending,
                SortedColumnsAttribute.getOrderForColumn(test, "Plant").orElse(null));
        TestCase.assertEquals(SortingOrder.Descending,
                SortedColumnsAttribute.getOrderForColumn(test, "Int").orElse(null));
        TestCase.assertEquals(Optional.empty(), SortedColumnsAttribute.getOrderForColumn(test, "D1"));
    }

    @Test
    public void testRefreshing() {
        final QueryTable test = TstUtils.testRefreshingTable(i(10, 11, 12, 17).toTracking(),
                stringCol("Plant", "Apple", "Banana", "Carrot", "Daffodil"),
                intCol("Int", 9, 7, 5, 3));

        final Table testPlant = TableAssertions.assertSorted("test", test, "Plant", SortingOrder.Ascending);
        final Table testInt = TableAssertions.assertSorted(test, "Int", SortingOrder.Descending);

        assertTableEquals(test, testPlant);
        assertTableEquals(test, testInt);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(test, i(11));
            addToTable(test, i(11), stringCol("Plant", "Berry"), intCol("Int", 6));
            test.notifyListeners(i(11), i(11), i());
        });

        assertTableEquals(test, testInt);
        assertTableEquals(test, testPlant);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(test, i(9, 13, 18), stringCol("Plant", "Aaple", "DAFODIL", "Forsythia"),
                    intCol("Int", 10, 4, 0));
            TableTools.showWithRowSet(test);
            test.notifyListeners(i(9, 13, 18), i(), i());
        });
    }

    @Test
    public void testIncremental() {
        for (int seed = 0; seed < 2; ++seed) {
            testIncrementalRandom(seed, 100);
        }
    }

    public void testIncrementalRandom(int seed, int size) {
        final Random random = new Random(seed);
        final int maxSteps = 100;

        // noinspection rawtypes
        final ColumnInfo[] columnInfo;
        final QueryTable table = getTable(true, size, random,
                columnInfo = initColumnInfos(new String[] {"SortValue", "Sentinel"},
                        new TstUtils.SortedLongGenerator(0, 1_000_000_000L),
                        new TstUtils.IntGenerator(0, 100000)));


        // This code could give you some level of confidence that we actually do work as intended; but is hard to test
        // try {
        // final Random random1 = new Random(0);
        // QueryScope.addParam("random1", random);

        // final Table badTable = UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() ->
        // table.update("RV=random1.nextDouble() < 0.00001 ? -1L : SortValue"));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return TableAssertions.assertSorted("table", table, "SortValue", SortingOrder.Ascending);
                    }
                },
                // new EvalNugget() {
                // @Override
                // protected Table e() {
                // return TableAssertions.assertSorted("badTable", badTable, "RV", SortingOrder.Ascending);
                // }
                // },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return TableAssertions.assertSorted("table sorted by sentinel",
                                table.sortDescending("Sentinel"), "Sentinel", SortingOrder.Descending);
                    }
                },
        };

        for (int step = 0; step < maxSteps; step++) {
            UpdateGraphProcessor.DEFAULT
                    .runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                            GenerateTableUpdates.DEFAULT_PROFILE, size, random, table, columnInfo));
            validate(en);
        }
        // } finally {
        // QueryScope.addParam("random1", null);
        // }
    }
}
