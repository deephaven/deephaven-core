/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.DateTime;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;

/**
 * Quick unit test for {@link SimulationClock}.
 */
public class TestSimulationClock extends RefreshingTableTestCase {

    public void testSignal() {
        final DateTime start = DateTime.now();
        final SimulationClock clock = new SimulationClock(start, DateTimeUtils.plus(start, 1), 1);
        clock.start();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int ci = 0; ci < 2; ++ci) {
            updateGraph.runWithinUnitTestCycle(clock::advance);
        }
        clock.awaitDoneUninterruptibly();
    }
}
