//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker.IntervalLevelDetails;

import java.io.IOException;

/**
 * Logs data that describes what a worker spent time on during its data refresh cycle.
 */
public interface UpdatePerformanceLogLogger {
    void log(final UpdatePerformanceTracker.IntervalLevelDetails intervalLevelDetails,
            final PerformanceEntry performanceEntry) throws IOException;

    enum Noop implements UpdatePerformanceLogLogger {
        INSTANCE;

        @Override
        public void log(IntervalLevelDetails intervalLevelDetails, PerformanceEntry performanceEntry)
                throws IOException {

        }
    }
}
