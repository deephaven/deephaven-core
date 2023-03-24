package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.tablelogger.Row;

import java.io.IOException;

public interface UpdatePerformanceLogLogger extends EngineTableLoggerProvider.EngineTableLogger {
    void log(final UpdatePerformanceTracker.IntervalLevelDetails intervalLevelDetails, final PerformanceEntry performanceEntry) throws IOException;

    void log(final Row.Flags flags, final UpdatePerformanceTracker.IntervalLevelDetails intervalLevelDetails, final PerformanceEntry performanceEntry) throws IOException;
}
