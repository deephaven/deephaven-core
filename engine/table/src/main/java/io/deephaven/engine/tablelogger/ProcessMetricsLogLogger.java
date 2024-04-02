//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.Row.Flags;

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

    enum Noop implements ProcessMetricsLogLogger {
        INSTANCE;

        @Override
        public void log(Flags flags, long timestamp, String processId, String name, String interval, String type,
                long n, long sum, long last, long min, long max, long avg, long sum2, long stdev) throws IOException {

        }
    }
}
