//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.verify;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SortedLongGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;

import java.util.Optional;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static junit.framework.TestCase.*;

public class TestTableAssertions {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testBlink() {
        final QueryTable test = TstUtils.testRefreshingTable(intCol("Sentinel"));

        final Table blink = test.assertBlink();
        final Table blink2 = blink.assertBlink();
        assertNotSame(blink, test);
        assertSame(blink, blink2);

        final ControlledUpdateGraph cug = ExecutionContext.getContext().getUpdateGraph().cast();
        cug.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(test, i(5), intCol("Sentinel", 5));
            test.notifyListeners(i(5), i(), i());
        });

        assertFalse(blink.isFailed());

        cug.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(test, i(5));
            TstUtils.addToTable(test, i(8), intCol("Sentinel", 8));
            test.notifyListeners(i(8), i(5), i());
        });

        assertFalse(blink.isFailed());
        assertTableEquals(test, blink);

        try (final SafeCloseable ignored = () -> base.setExpectError(false)) {
            base.setExpectError(true);

            cug.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(test, i(9), intCol("Sentinel", 9));
                test.notifyListeners(i(9), i(), i());
            });
            assertFalse(test.isFailed());
            assertTrue(blink.isFailed());

            assertEquals(1, base.getUpdateErrors().size());
            assertTrue(base.getUpdateErrors().get(0) instanceof AssertionFailure);
            assertEquals(
                    "Assertion failed: asserted added size == current table size, instead added size == 1, current table size == 2.",
                    base.getUpdateErrors().get(0).getMessage());
        }
    }

    @Test
    public void testAddOnly() {
        final QueryTable test = TstUtils.testRefreshingTable(intCol("Sentinel"));

        final Table ao1 = test.assertAddOnly();
        final Table ao2 = ao1.assertAddOnly();
        assertNotSame(ao1, test);
        assertSame(ao1, ao2);

        final ControlledUpdateGraph cug = ExecutionContext.getContext().getUpdateGraph().cast();
        cug.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(test, i(5), intCol("Sentinel", 5));
            test.notifyListeners(i(5), i(), i());
        });

        // differentiate from append only
        cug.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(test, i(4), intCol("Sentinel", 4));
            test.notifyListeners(i(4), i(), i());
        });

        assertFalse(ao1.isFailed());

        try (final SafeCloseable ignored = () -> base.setExpectError(false)) {
            base.setExpectError(true);

            cug.runWithinUnitTestCycle(() -> {
                TstUtils.removeRows(test, i(5));
                TstUtils.addToTable(test, i(8), intCol("Sentinel", 8));
                test.notifyListeners(i(8), i(5), i());
            });

            assertFalse(test.isFailed());
            assertTrue(ao1.isFailed());

            assertEquals(1, base.getUpdateErrors().size());
            assertTrue(base.getUpdateErrors().get(0) instanceof AssertionFailure);
            assertEquals("Assertion failed: asserted updateToSend.removed.empty().",
                    base.getUpdateErrors().get(0).getMessage());
        }
    }

    @Test
    public void testAppendOnly() {
        final QueryTable test = TstUtils.testRefreshingTable(intCol("Sentinel"));

        final Table test2 = TstUtils.testRefreshingTable(intCol("Sentinel")).assertAddOnly();
        ((QueryTable) (test2)).setFlat();
        assertTrue(((QueryTable) (test2)).isAddOnly());
        assertTrue(((QueryTable) (test2)).isAppendOnly());
        // test2 is append only, but doesn't have the attribute set; so we will return a new table
        assertNotSame(test2.assertAppendOnly(), test2);
        assertSame(test2.assertAddOnly(), test2);

        final Table ao1 = test.assertAppendOnly();
        final Table ao2 = ao1.assertAppendOnly();
        assertNotSame(ao1, test);
        assertSame(ao1, ao2);

        final ControlledUpdateGraph cug = ExecutionContext.getContext().getUpdateGraph().cast();
        cug.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(test, i(5), intCol("Sentinel", 5));
            test.notifyListeners(i(5), i(), i());
        });

        cug.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(test, i(6), intCol("Sentinel", 6));
            test.notifyListeners(i(6), i(), i());
        });

        assertFalse(ao1.isFailed());

        try (final SafeCloseable ignored = () -> base.setExpectError(false)) {
            base.setExpectError(true);

            cug.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(test, i(2), intCol("Sentinel", 2));
                test.notifyListeners(i(2), i(), i());
            });

            assertFalse(test.isFailed());
            assertTrue(ao1.isFailed());

            assertEquals(1, base.getUpdateErrors().size());
            assertTrue(base.getUpdateErrors().get(0) instanceof AssertionFailure);
            assertEquals(
                    "Assertion failed: asserted getRowSet().lastRowKeyPrev() < updateToSend.added().firstRowKey().",
                    base.getUpdateErrors().get(0).getMessage());
        }
    }

    @Test
    public void testStatic() {
        final Table test = TableTools.newTable(stringCol("Plant", "Apple", "Banana", "Carrot", "Daffodil"),
                intCol("Int", 9, 3, 2, 1),
                doubleCol("D1", QueryConstants.NULL_DOUBLE, Math.E, Math.PI, Double.NEGATIVE_INFINITY));

        final Table withSortedPlant = TableAssertions.assertSorted("test", test, "Plant", SortingOrder.Ascending);
        TestCase.assertSame(test.getRowSet(), withSortedPlant.getRowSet());
        final Table withSortedInt = TableAssertions.assertSorted(withSortedPlant, "Int", SortingOrder.Descending);
        TestCase.assertSame(test.getRowSet(), withSortedInt.getRowSet());
        try {
            TableAssertions.assertSorted("test", test, "D1", SortingOrder.Ascending);
            TestCase.fail("Table is not actually sorted by D1");
        } catch (SortedAssertionFailure saf) {
            TestCase.assertEquals(
                    "Table violates sorted assertion, table description=test, column=D1, Ascending, 3.141592653589793 is out of order with respect to -Infinity!",
                    saf.getMessage());
        }

        TestCase.assertEquals(SortingOrder.Ascending,
                SortedColumnsAttribute.getOrderForColumn(withSortedPlant, "Plant").orElse(null));
        TestCase.assertEquals(SortingOrder.Descending,
                SortedColumnsAttribute.getOrderForColumn(withSortedInt, "Int").orElse(null));
        TestCase.assertEquals(Optional.empty(), SortedColumnsAttribute.getOrderForColumn(withSortedInt, "D1"));
    }

    @Test
    public void testRefreshing() {
        final QueryTable test = testRefreshingTable(i(10, 11, 12, 17).toTracking(),
                stringCol("Plant", "Apple", "Banana", "Carrot", "Daffodil"),
                intCol("Int", 9, 7, 5, 3));

        final Table testPlant = TableAssertions.assertSorted("test", test, "Plant", SortingOrder.Ascending);
        final Table testInt = TableAssertions.assertSorted(test, "Int", SortingOrder.Descending);

        assertTableEquals(test, testPlant);
        assertTableEquals(test, testInt);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(test, i(11));
            addToTable(test, i(11), stringCol("Plant", "Berry"), intCol("Int", 6));
            test.notifyListeners(i(11), i(11), i());
        });

        assertTableEquals(test, testInt);
        assertTableEquals(test, testPlant);

        updateGraph.runWithinUnitTestCycle(() -> {
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
                        new SortedLongGenerator(0, 1_000_000_000L),
                        new IntGenerator(0, 100000)));


        // This code could give you some level of confidence that we actually do work as intended; but is hard to test
        // try {
        // final Random random1 = new Random(0);
        // QueryScope.addParam("random1", random);

        // final Table badTable = ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(() ->
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

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < maxSteps; step++) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, table, columnInfo));
            validate(en);
        }
        // } finally {
        // QueryScope.addParam("random1", null);
        // }
    }
}
