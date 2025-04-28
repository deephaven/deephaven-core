//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import io.deephaven.tablelogger.Row.Flags;

import java.io.IOException;

import static io.deephaven.tablelogger.TableLogger.DEFAULT_INTRADAY_LOGGER_FLAGS;

/**
 * Logs data that describes the relationships between a {@link io.deephaven.engine.table.impl.perf.PerformanceEntry} and
 * its ancestors.
 */
public interface UpdatePerformanceAncestorLogger {
    default void log(final String updateGraphName, final long id, final String description, final long[] ancestors)
            throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, updateGraphName, id, description, ancestors);
    }

    void log(final Flags flags, final String updateGraphName, final long id, final String description,
            final long[] ancestors) throws IOException;

    enum Noop implements UpdatePerformanceAncestorLogger {
        INSTANCE;

        @Override
        public void log(Flags flags, final String updateGraphName, final long id, final String description,
                final long[] ancestors) {}
    }
}
