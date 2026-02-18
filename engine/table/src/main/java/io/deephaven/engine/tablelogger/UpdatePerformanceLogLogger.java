//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker.IntervalLevelDetails;
import io.deephaven.tablelogger.Row;

import java.io.IOException;

/**
 * Logs data that describes what a worker spent time on during its data refresh cycle.
 */
public interface UpdatePerformanceLogLogger {
    void log(final UpdatePerformanceTracker.IntervalLevelDetails intervalLevelDetails,
            final PerformanceEntry performanceEntry) throws IOException;

    @Deprecated
    default void log(final Row.Flags flags, final UpdatePerformanceTracker.IntervalLevelDetails intervalLevelDetails,
            final PerformanceEntry performanceEntry) throws IOException {
        throw new UnsupportedOperationException();
    }

    enum Noop implements UpdatePerformanceLogLogger {
        INSTANCE;

        @Override
        public void log(IntervalLevelDetails intervalLevelDetails, PerformanceEntry performanceEntry)
                throws IOException {

        }
    }
}
