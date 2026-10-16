//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.testutil.TstUtils.assertRowSetEquals;

public class RollingReleaseFilterTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testWindowAdvancesAndWraps() {
        final Table source = TableTools.emptyTable(100).update("X = ii");
        final RollingReleaseFilter filter = new RollingReleaseFilter(20, 30);
        final Table filtered = source.where(filter);
        assertRowSetEquals(RowSetFactory.fromRange(0, 19), filtered.getRowSet());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(filter::run);
        assertRowSetEquals(RowSetFactory.fromRange(30, 49), filtered.getRowSet());

        updateGraph.runWithinUnitTestCycle(filter::run);
        assertRowSetEquals(RowSetFactory.fromRange(60, 79), filtered.getRowSet());

        // the window starting at 90 wraps around the end of the table
        updateGraph.runWithinUnitTestCycle(filter::run);
        final WritableRowSet expected = RowSetFactory.fromRange(0, 9);
        expected.insertRange(90, 99);
        assertRowSetEquals(expected, filtered.getRowSet());
    }
}
