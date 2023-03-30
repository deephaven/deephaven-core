package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryProcessingResults;
import io.deephaven.tablelogger.Row;

import java.io.IOException;

/**
 * Logs data that describes the query-level performance for each worker. A given worker may be running multiple queries;
 * each will have its own set of query performance log entries.
 */
import static io.deephaven.tablelogger.TableLogger.DEFAULT_INTRADAY_LOGGER_FLAGS;

public interface QueryPerformanceLogLogger {
    default void log(final long evaluationNumber, final QueryProcessingResults queryProcessingResults,
            final QueryPerformanceNugget nugget) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, evaluationNumber, queryProcessingResults, nugget);
    }

    void log(final Row.Flags flags, final long evaluationNumber, final QueryProcessingResults queryProcessingResults,
            final QueryPerformanceNugget nugget) throws IOException;
}
