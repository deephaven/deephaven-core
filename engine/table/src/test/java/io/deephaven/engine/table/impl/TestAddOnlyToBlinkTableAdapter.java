//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestAddOnlyToBlinkTableAdapter {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private CapturingUpdateGraph updateGraph;
    private SafeCloseable contextCloseable;

    @Before
    public void setup() throws Exception {
        updateGraph = new CapturingUpdateGraph(ExecutionContext.getContext().getUpdateGraph().cast());
        contextCloseable = updateGraph.getContext().open();
    }

    @After
    public void tearDown() throws Exception {
        contextCloseable.close();
    }

    @Test
    public void testAddOnlyToBlinkAdapter() {
        final QueryTable table = testRefreshingTable(stringCol("Col", "A", "B", "C"),
                intCol("Ints", 1, 2, 3));
        table.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);

        final Table blinky = AddOnlyToBlinkTableAdapter.toBlink(table);
        assertEquals(Boolean.TRUE, blinky.getAttribute(Table.BLINK_TABLE_ATTRIBUTE));
        assertTableEquals(table, blinky);

        updateGraph.runWithinUnitTestCycle(() -> updateGraph.refreshSources(), true);
        assertTrue(blinky.isEmpty());

        updateGraph.runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            final WritableRowSet added = i(3, 4, 5);
            addToTable(table, added,
                    stringCol("Col", "D", "E", "F"),
                    intCol("Ints", 4, 5, 6));
            table.notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        }, true);

        assertTableEquals(table.getSubTable(i(3, 4, 5).toTracking()), blinky);

        updateGraph.runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            final WritableRowSet added = i(6, 7, 8);
            addToTable(table, added,
                    stringCol("Col", "G", "H", "I"),
                    intCol("Ints", 7, 8, 9));
            table.notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        }, true);

        assertTableEquals(table.getSubTable(i(6, 7, 8).toTracking()), blinky);

        updateGraph.runWithinUnitTestCycle(() -> updateGraph.refreshSources(), true);
        assertTrue(blinky.isEmpty());
    }
}
