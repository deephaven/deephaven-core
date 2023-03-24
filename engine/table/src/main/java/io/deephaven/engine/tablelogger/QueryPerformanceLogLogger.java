package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryProcessingResults;
import io.deephaven.tablelogger.Row;

import java.io.IOException;

public interface QueryPerformanceLogLogger extends EngineTableLoggerProvider.EngineTableLogger {
    void log(final long evaluationNumber, final QueryProcessingResults queryProcessingResults, final QueryPerformanceNugget nugget) throws IOException;

    void log(
            final Row.Flags flags, final long evaluationNumber,
            final QueryProcessingResults queryProcessingResults, final QueryPerformanceNugget nugget)
            throws IOException;
}
