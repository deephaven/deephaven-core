//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;

import java.time.Instant;

/**
 * Quick unit test for {@link SimulationClock}.
 */
public class TestSimulationClock extends RefreshingTableTestCase {

    public void testSignal() {
        final Instant start = DateTimeUtils.now();
        final SimulationClock clock = new SimulationClock(start, start.plusNanos(1), 1);
        clock.start();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int ci = 0; ci < 2; ++ci) {
            updateGraph.runWithinUnitTestCycle(clock::advance);
        }
        clock.awaitDoneUninterruptibly();
    }
}
