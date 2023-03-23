package io.deephaven.engine.tablelogger;

import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.tablelogger.Row;

import java.io.IOException;

public interface QueryOperationPerformanceLogLoggerInterface {
    void log(final int operationNumber, final QueryPerformanceNugget nugget) throws IOException;

    void log(final Row.Flags flags, final int operationNumber, final QueryPerformanceNugget nugget) throws IOException;
}
