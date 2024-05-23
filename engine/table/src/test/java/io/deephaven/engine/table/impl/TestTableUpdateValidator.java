//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.exceptions.TableAlreadyFailedException;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.FloatGenerator;
import io.deephaven.engine.util.PrintListener;
import io.deephaven.util.mutable.MutableInt;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.*;
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
            final Table table2 = table1.updateView("Y=x*mult.intValue()");
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
            mult.setValue(3);

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
}
