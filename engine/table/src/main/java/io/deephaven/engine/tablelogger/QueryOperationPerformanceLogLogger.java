package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.Row.Flags;

import java.io.IOException;

import static io.deephaven.tablelogger.TableLogger.DEFAULT_INTRADAY_LOGGER_FLAGS;

/**
 * Logs data that describes performance details on initialization times and memory usage of specific operations within
 * queries.
 */
public interface QueryOperationPerformanceLogLogger {
    default void log(final QueryPerformanceNugget nugget) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, nugget);
    }

    void log(Row.Flags flags, QueryPerformanceNugget nugget) throws IOException;

    enum Noop implements QueryOperationPerformanceLogLogger {
        INSTANCE;

        @Override
        public void log(Flags flags, QueryPerformanceNugget nugget) throws IOException {

        }
    }
}
