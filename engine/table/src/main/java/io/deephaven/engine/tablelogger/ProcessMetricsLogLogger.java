package io.deephaven.engine.tablelogger;

import io.deephaven.tablelogger.Row;

import java.io.IOException;

import static io.deephaven.tablelogger.TableLogger.DEFAULT_INTRADAY_LOGGER_FLAGS;

/**
 * Logs data that gives insight to process statistics such as heap usage and page fault counts.
 */
public interface ProcessMetricsLogLogger {
    default void log(final long timestamp, final String processId, final String name, final String interval,
            final String type, final long n, final long sum, final long last, final long min, final long max,
            final long avg, final long sum2, final long stdev) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, timestamp, processId, name, interval, type, n, sum, last, min, max, avg,
                sum2, stdev);
    }

    void log(final Row.Flags flags, final long timestamp, final String processId, final String name,
            final String interval, final String type, final long n, final long sum, final long last, final long min,
            final long max, final long avg, final long sum2, final long stdev) throws IOException;
}
