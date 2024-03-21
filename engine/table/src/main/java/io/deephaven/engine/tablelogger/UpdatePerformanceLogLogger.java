//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker.IntervalLevelDetails;
import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.Row.Flags;

import java.io.IOException;

import static io.deephaven.tablelogger.TableLogger.DEFAULT_INTRADAY_LOGGER_FLAGS;

/**
 * Logs data that describes what a worker spent time on during its data refresh cycle.
 */
public interface UpdatePerformanceLogLogger {
    default void log(final UpdatePerformanceTracker.IntervalLevelDetails intervalLevelDetails,
            final PerformanceEntry performanceEntry) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, intervalLevelDetails, performanceEntry);
    }

    void log(final Row.Flags flags, final UpdatePerformanceTracker.IntervalLevelDetails intervalLevelDetails,
            final PerformanceEntry performanceEntry) throws IOException;

    enum Noop implements UpdatePerformanceLogLogger {
        INSTANCE;

        @Override
        public void log(Flags flags, IntervalLevelDetails intervalLevelDetails, PerformanceEntry performanceEntry) {

        }
    }
}
