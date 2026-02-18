//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import io.deephaven.tablelogger.Row;

import java.io.IOException;

/**
 * Logs data that describes the relationships between a {@link io.deephaven.engine.table.impl.perf.PerformanceEntry} and
 * its ancestors.
 */
public interface UpdatePerformanceAncestorLogger {
    void log(final String updateGraphName, final long id, final String description, final long[] ancestors)
            throws IOException;

    @Deprecated(forRemoval = true)
    default void log(final Row.Flags flags, final String updateGraphName, final long id, final String description,
            final long[] ancestors) throws IOException {
        throw new UnsupportedOperationException();
    }

    enum Noop implements UpdatePerformanceAncestorLogger {
        INSTANCE;

        @Override
        public void log(String updateGraphName, long id, String description, long[] ancestors) throws IOException {

        }
    }
}
