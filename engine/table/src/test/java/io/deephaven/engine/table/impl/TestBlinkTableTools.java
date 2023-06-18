/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.util.TableTools;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;

import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;

public class TestBlinkTableTools {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testBlinkToAppendOnlyTable() {
        final Instant dt1 = DateTimeUtils.parseInstant("2021-08-11T8:20:00 NY");
        final Instant dt2 = DateTimeUtils.parseInstant("2021-08-11T8:21:00 NY");
        final Instant dt3 = DateTimeUtils.parseInstant("2021-08-11T11:22:00 NY");

        final QueryTable blinkTable = TstUtils.testRefreshingTable(i(1).toTracking(), intCol("I", 7),
                doubleCol("D", Double.NEGATIVE_INFINITY), instantCol("DT", dt1), col("B", Boolean.TRUE));
        blinkTable.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);

        final Table appendOnly = BlinkTableTools.blinkToAppendOnly(blinkTable);

        assertTableEquals(blinkTable, appendOnly);
        TestCase.assertEquals(true, appendOnly.getAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE));
        TestCase.assertTrue(appendOnly.isFlat());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            RowSet removed1 = blinkTable.getRowSet().copyPrev();
            ((WritableRowSet) blinkTable.getRowSet()).clear();
            TstUtils.addToTable(blinkTable, i(7), intCol("I", 1), doubleCol("D", Math.PI), instantCol("DT", dt2),
                    col("B", true));
            blinkTable.notifyListeners(i(7), removed1, i());
        });

        assertTableEquals(TableTools.newTable(intCol("I", 7, 1), doubleCol("D", Double.NEGATIVE_INFINITY, Math.PI),
                instantCol("DT", dt1, dt2), col("B", true, true)), appendOnly);

        updateGraph.runWithinUnitTestCycle(() -> {
            RowSet removed = blinkTable.getRowSet().copyPrev();
            ((WritableRowSet) blinkTable.getRowSet()).clear();
            TstUtils.addToTable(blinkTable, i(7), intCol("I", 2), doubleCol("D", Math.E), instantCol("DT", dt3),
                    col("B", false));
            blinkTable.notifyListeners(i(7), removed, i());
        });
        assertTableEquals(
                TableTools.newTable(intCol("I", 7, 1, 2), doubleCol("D", Double.NEGATIVE_INFINITY, Math.PI, Math.E),
                        instantCol("DT", dt1, dt2, dt3), col("B", true, true, false)),
                appendOnly);
    }

}
