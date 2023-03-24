package io.deephaven.engine.tablelogger;

import io.deephaven.tablelogger.Row;

import java.io.IOException;

public interface ProcessMetricsLogLogger extends EngineTableLoggerProvider.EngineTableLogger {
    void log(final long timestamp, final String processId, final String name, final String interval, final String type, final long n, final long sum, final long last, final long min, final long max, final long avg, final long sum2, final long stdev) throws IOException;

    void log(final Row.Flags flags, final long timestamp, final String processId, final String name, final String interval, final String type, final long n, final long sum, final long last, final long min, final long max, final long avg, final long sum2, final long stdev) throws IOException;
}
