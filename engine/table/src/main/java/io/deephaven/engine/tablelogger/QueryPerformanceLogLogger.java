//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.tablelogger.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

/**
 * Logs data that describes the query-level performance for each worker. A given worker may be running multiple queries;
 * each will have its own set of query performance log entries.
 */
public interface QueryPerformanceLogLogger {
    void log(
            @NotNull final QueryPerformanceNugget nugget,
            @Nullable final Exception exception) throws IOException;

    @Deprecated(forRemoval = true)
    default void log(
            @NotNull final Row.Flags flags,
            @NotNull final QueryPerformanceNugget nugget,
            @Nullable final Exception exception) throws IOException {
        throw new UnsupportedOperationException();
    }

    enum Noop implements QueryPerformanceLogLogger {
        INSTANCE;

        @Override
        public void log(@NotNull QueryPerformanceNugget nugget, @Nullable Exception exception) throws IOException {

        }
    }
}
