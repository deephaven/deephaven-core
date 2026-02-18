//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.tablelogger.Row;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * Logs data that describes performance details on initialization times and memory usage of specific operations within
 * queries.
 */
public interface QueryOperationPerformanceLogLogger {
    void log(@NotNull final QueryPerformanceNugget nugget) throws IOException;

    @Deprecated(forRemoval = true)
    default void log(@NotNull Row.Flags flags, @NotNull QueryPerformanceNugget nugget) throws IOException {
        throw new UnsupportedOperationException();
    }

    enum Noop implements QueryOperationPerformanceLogLogger {
        INSTANCE;


        @Override
        public void log(@NotNull QueryPerformanceNugget nugget) throws IOException {

        }
    }
}
