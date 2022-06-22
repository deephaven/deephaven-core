/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.RefreshingTableTestCase;

/**
 * Quick unit test for {@link SimulationClock}.
 */
public class TestSimulationClock extends RefreshingTableTestCase {

    public void testSignal() {
        final DateTime start = DateTime.now();
        final SimulationClock clock = new SimulationClock(start, DateTimeUtils.plus(start, 1), 1);
        clock.start();
        for (int ci = 0; ci < 2; ++ci) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(clock::advance);
        }
        clock.awaitDoneUninterruptibly();
    }
}
