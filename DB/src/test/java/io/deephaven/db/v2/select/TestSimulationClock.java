package io.deephaven.db.v2.select;

import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.LiveTableTestCase;

/**
 * Quick unit test for {@link SimulationClock}.
 */
public class TestSimulationClock extends LiveTableTestCase {

    public void testSignal() {
        final DBDateTime start = DBDateTime.now();
        final SimulationClock clock = new SimulationClock(start, DBTimeUtils.plus(start, 1), 1);
        clock.start();
        for (int ci = 0; ci < 2; ++ci) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(clock::advance);
        }
        clock.awaitDoneUninterruptibly();
    }
}
