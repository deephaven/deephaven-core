//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.exceptions.TableAlreadyFailedException;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.FloatGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.util.PrintListener;
import io.deephaven.util.mutable.MutableInt;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.intCol;

/**
 * Test that the TableUpdateValidator can be put in the middle of an operation.
 */
public class TestTableUpdateValidator extends QueryTableTestBase {
    @Test
    public void testPassThrough() {
        final Random random = new Random(0);

        final int size = 500;

        final ColumnInfo[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"D1", "D2", "F1", "F2"},
                        new DoubleGenerator(),
                        new DoubleGenerator(0, 1000, 0.1, 0.1),
                        new FloatGenerator(),
                        new FloatGenerator(0, 1000, 0.1, 0.1)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new TableComparator(table, TableUpdateValidator.make(table).getResultTable()),
        };

        for (int i = 0; i < 500; i++) {
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    @Test
    public void testPassThroughShift() {
        final QueryTable table =
                testRefreshingTable(i(10, 20, 30).toTracking(), intCol("A", 1, 2, 3), intCol("B", 3, 4, 5));
        final QueryTable table2 = TableUpdateValidator.make(table).getResultTable();
        final FailureListener failureListener = new FailureListener();
        table2.addUpdateListener(failureListener);
        final PrintListener printListener = new PrintListener("table2", table);

        ControlledUpdateGraph cast = table.getUpdateGraph().cast();
        cast.runWithinUnitTestCycle(() -> {
            addToTable(table, i(20), intCol("A", 2), intCol("B", 6));
            final TableUpdateImpl update = new TableUpdateImpl();
            update.shifted = RowSetShiftData.EMPTY;
            update.modifiedColumnSet = table.newModifiedColumnSet("B");
            update.added = i();
            update.modified = i(20);
            update.removed = i();
            table.notifyListeners(update);
        });
    }

    @Test
    public void testInvalid() {
        final MutableInt mult = new MutableInt(2);

        final QueryTable table1 = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), intCol("x", 1, 2, 3));
        QueryScope.addParam("mult", mult);
        try {
            final Table table2 = table1.updateView("Y=x*mult.get()");
            final QueryTable table3 = TableUpdateValidator.make((QueryTable) table2).getResultTable();

            ControlledUpdateGraph updateGraph = table1.updateGraph.cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(table1, i(6), intCol("x", 4));
                table1.notifyListeners(i(), i(), i(6));
            });

            assertTableEquals(table2, table3);

            final SimpleListener listener = new SimpleListener(table3);
            table3.addUpdateListener(listener);
            table3.removeUpdateListener(listener);

            // next should fail because we set mult
            mult.set(3);

            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(table1, i(6), intCol("x", 5));
                table1.notifyListeners(i(), i(), i(6));
            });

            final SimpleListener listener2 = new SimpleListener(table3);
            try {
                table3.addUpdateListener(listener2);
                fail("expected exception");
            } catch (TableAlreadyFailedException e) {
                assertEquals("Can not listen to failed table QueryTable", e.getMessage());
            }
        } finally {
            QueryScope.addParam("mult", null);
        }
    }

    @Test
    public void testMCS() {
        final MutableInt mult = new MutableInt(2);

        final QueryTable table1 = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(), intCol("x", 1, 2, 3));
        QueryScope.addParam("mult", mult);
        try {
            final Table table2 = table1.updateView("Y=x*mult.get()");
            final QueryTable table3 = TableUpdateValidator.make((QueryTable) table2).getResultTable();

            final Table table4 = table3.updateView("Z=Y*2");

            ControlledUpdateGraph updateGraph = table1.updateGraph.cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(table1, i(6), intCol("x", 4));
                table1.notifyListeners(i(), i(), i(6));
            });

            assertTableEquals(table1.updateView("Y=x*2", "Z=Y*2"), table4);
        } finally {
            QueryScope.addParam("mult", null);
        }
    }

    /**
     * A table whose initial row set is scattered across the row-key space forces the validator to store expected values
     * through a row redirection from initialization; exercise adds, modifies, removes, and free-slot reuse.
     */
    @Test
    public void testRedirectedFromInitialization() {
        final long big = 1L << 41;
        final QueryTable table = testRefreshingTable(
                i(0, 5, big, big + 5, 2 * big).toTracking(),
                intCol("I", 1, 2, 3, 4, 5),
                doubleCol("D", 1.5, 2.5, 3.5, 4.5, 5.5),
                col("S", "a", "b", "c", "d", "e"));

        final TableUpdateValidator tuv = TableUpdateValidator.make("scattered keys", table);
        final TableUpdateListener failureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(failureListener);
        assertTrue(tuv.isRedirectionUsed());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // added rows at fresh scattered keys
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(3 * big, 3 * big + 7),
                    intCol("I", 6, 7), doubleCol("D", 6.5, 7.5), col("S", "f", "g"));
            table.notifyListeners(i(3 * big, 3 * big + 7), i(), i());
        });

        // modified rows
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(big, 2 * big),
                    intCol("I", 30, 50), doubleCol("D", 30.5, 50.5), col("S", "C", "E"));
            table.notifyListeners(i(), i(), i(big, 2 * big));
        });

        // removed rows; their inner slots go to the free list
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(0, big + 5));
            table.notifyListeners(i(), i(0, big + 5), i());
        });

        // re-add one of the removed keys plus a fresh key; reuses freed inner slots
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(big + 5, 4 * big),
                    intCol("I", 8, 9), doubleCol("D", 8.5, 9.5), col("S", "h", "i"));
            table.notifyListeners(i(big + 5, 4 * big), i(), i());
        });

        tuv.deepValidation();
        assertFalse(tuv.hasFailed());
    }

    /**
     * A table that starts with a dense row set records expected values in the sparse representation; adding rows at
     * scattered keys must migrate the recorded values to the redirected representation, after which updates to both old
     * (copied) and new rows must validate.
     */
    @Test
    public void testSwitchToRedirected() {
        final QueryTable table = testRefreshingTable(
                i(0, 1, 2, 3).toTracking(),
                intCol("I", 1, 2, 3, 4),
                doubleCol("D", 1.5, 2.5, 3.5, 4.5),
                col("S", "a", "b", "c", "d"));

        final TableUpdateValidator tuv = TableUpdateValidator.make("dense start", table);
        final TableUpdateListener failureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(failureListener);
        assertFalse(tuv.isRedirectionUsed());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // modify a row so the migration has recorded values to copy
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(1), intCol("I", 20), doubleCol("D", 20.5), col("S", "B"));
            table.notifyListeners(i(), i(), i(1));
        });
        assertFalse(tuv.isRedirectionUsed());

        // scatter rows across the key space; the validator must migrate at the end of this cycle
        final long big = 1L << 41;
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(big, 2 * big, 3 * big),
                    intCol("I", 5, 6, 7), doubleCol("D", 5.5, 6.5, 7.5), col("S", "e", "f", "g"));
            table.notifyListeners(i(big, 2 * big, 3 * big), i(), i());
        });
        assertTrue(tuv.isRedirectionUsed());

        // post-migration updates touching both copied and new rows
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2, 2 * big), intCol("I", 30, 60), doubleCol("D", 30.5, 60.5), col("S", "C", "F"));
            removeRows(table, i(0));
            table.notifyListeners(i(), i(0), i(2, 2 * big));
        });

        tuv.deepValidation();
        assertFalse(tuv.hasFailed());
    }

    /**
     * An ungrouped sorted table reproduces the production shape that motivated the redirected representation: sorted
     * parents live near {@code 2^30} and ungrouping shifts them up by the ungroup base, scattering one small group per
     * sparse block. Random incremental updates drive real ungroup-generated shift data through the redirected mapping.
     */
    @Test
    public void testRedirectedWithShifts() {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(100, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new DoubleGenerator(0, 100)));

        final QueryTable ungrouped = (QueryTable) table.groupBy("Sym").sort("Sym").ungroup();
        final TableUpdateValidator tuv = TableUpdateValidator.make("ungrouped", ungrouped);
        final TableUpdateListener failureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(failureListener);

        for (int step = 0; step < 50; ++step) {
            simulateShiftAwareStep(10, random, table, columnInfo, new EvalNuggetInterface[0]);
        }

        tuv.deepValidation();
        assertFalse(tuv.hasFailed());
        assertTrue(tuv.isRedirectionUsed());
    }
}
