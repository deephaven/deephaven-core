//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import java.io.IOException;

/**
 * Logs data that describes the relationships between a {@link io.deephaven.engine.table.impl.perf.PerformanceEntry} and
 * its ancestors.
 */
public interface UpdatePerformanceAncestorLogger {
    void log(final String updateGraphName, final long id, final String description, final long[] ancestors)
            throws IOException;

    enum Noop implements UpdatePerformanceAncestorLogger {
        INSTANCE;

        @Override
        public void log(String updateGraphName, long id, String description, long[] ancestors) throws IOException {

        }
    }
}
