//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestValidateUpdateOverlaps {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testAddedOverlapsModified() {
        final QueryTable table = testRefreshingTable(i(2, 4).toTracking(), intCol("X", 1, 2));
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final AssertionFailure failure = assertThrows(AssertionFailure.class,
                () -> updateGraph.runWithinUnitTestCycle(() -> {
                    TstUtils.addToTable(table, i(6), intCol("X", 3));
                    table.notifyListeners(i(6), i(), i(4, 6));
                }));
        assertTrue(failure.getMessage(), failure.getMessage().contains("addedOverlapsModified"));
        assertTrue(failure.getMessage(), failure.getMessage().contains("addedIntersectModified={6}"));
    }

    @Test
    public void testAddedDisjointFromModified() {
        final QueryTable table = testRefreshingTable(i(2, 4).toTracking(), intCol("X", 1, 2));
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, i(4, 6), intCol("X", 5, 3));
            table.notifyListeners(i(6), i(), i(4));
        });
        assertEquals(3, table.size());
    }
}
