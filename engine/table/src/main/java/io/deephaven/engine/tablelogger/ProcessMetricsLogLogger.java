//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import java.io.Closeable;
import java.io.IOException;

/**
 * Logs data that gives insight to process statistics such as heap usage and page fault counts.
 */
public interface ProcessMetricsLogLogger extends Closeable {
    void log(final long timestamp, final String processId, final String name, final String interval,
            final String type, final long n, final long sum, final long last, final long min, final long max,
            final long avg, final long sum2, final long stdev) throws IOException;

    enum Noop implements ProcessMetricsLogLogger {
        INSTANCE;

        @Override
        public void log(long timestamp, String processId, String name, String interval, String type, long n, long sum,
                long last, long min, long max, long avg, long sum2, long stdev) throws IOException {

        }

        @Override
        public void close() throws IOException {

        }
    }
}
