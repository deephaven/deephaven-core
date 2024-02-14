package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.tablelogger.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

import static io.deephaven.tablelogger.TableLogger.DEFAULT_INTRADAY_LOGGER_FLAGS;

/**
 * Logs data that describes the query-level performance for each worker. A given worker may be running multiple queries;
 * each will have its own set of query performance log entries.
 */
public interface QueryPerformanceLogLogger {
    default void log(
            @NotNull final QueryPerformanceNugget nugget,
            @Nullable final Exception exception) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, nugget, exception);
    }

    void log(
            @NotNull final Row.Flags flags,
            @NotNull final QueryPerformanceNugget nugget,
            @Nullable final Exception exception) throws IOException;

    enum Noop implements QueryPerformanceLogLogger {
        INSTANCE;

        @Override
        public void log(
                @NotNull final Row.Flags flags,
                @NotNull final QueryPerformanceNugget nugget,
                @Nullable final Exception exception) {}
    }
}
