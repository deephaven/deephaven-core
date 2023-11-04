package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.Row.Flags;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

import static io.deephaven.tablelogger.TableLogger.DEFAULT_INTRADAY_LOGGER_FLAGS;

/**
 * Logs data that describes performance details on initialization times and memory usage of specific operations within
 * queries.
 */
public interface QueryOperationPerformanceLogLogger {
    default void log(@NotNull final QueryPerformanceNugget nugget) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, nugget);
    }

    default void log(
            @NotNull final Row.Flags flags,
            @NotNull final QueryPerformanceNugget nugget) throws IOException {
        log(flags, nugget.getOperationNumber(), nugget);
    }

    // This prototype is going to be deprecated in 0.31 in favor of the one above.
    void log(Row.Flags flags, int operationNumber, QueryPerformanceNugget nugget) throws IOException;

    enum Noop implements QueryOperationPerformanceLogLogger {
        INSTANCE;

        @Override
        public void log(
                @NotNull final Flags flags,
                final int operationNumber,
                @NotNull final QueryPerformanceNugget nugget) throws IOException {

        }
    }
}
