//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger.impl.memory;

import io.deephaven.engine.tablelogger.EngineTableLoggers;
import io.deephaven.engine.tablelogger.ProcessInfoLogLogger;
import io.deephaven.engine.tablelogger.ProcessMetricsLogLogger;
import io.deephaven.engine.tablelogger.QueryOperationPerformanceLogLogger;
import io.deephaven.engine.tablelogger.QueryPerformanceLogLogger;
import io.deephaven.engine.tablelogger.ServerStateLogLogger;
import io.deephaven.engine.tablelogger.UpdatePerformanceLogLogger;

/**
 * Provides memory table logger implementations for the engine table loggers.
 *
 * <p>
 * Deprecated, use {@link EngineTableLoggers.Factory.Noop#INSTANCE}.
 */
@Deprecated(since = "0.26.0", forRemoval = true)
public class EngineTableLoggersFactoryMemoryImpl implements EngineTableLoggers.Factory {
    @Override
    public ProcessInfoLogLogger processInfoLogLogger() {
        return ProcessInfoLogLogger.Noop.INSTANCE;
    }

    @Override
    public ProcessMetricsLogLogger processMetricsLogLogger() {
        return ProcessMetricsLogLogger.Noop.INSTANCE;
    }

    @Override
    public QueryOperationPerformanceLogLogger queryOperationPerformanceLogLogger() {
        return QueryOperationPerformanceLogLogger.Noop.INSTANCE;
    }

    @Override
    public QueryPerformanceLogLogger queryPerformanceLogLogger() {
        return QueryPerformanceLogLogger.Noop.INSTANCE;
    }

    @Override
    public ServerStateLogLogger serverStateLogLogger() {
        return ServerStateLogLogger.Noop.INSTANCE;
    }

    @Override
    public UpdatePerformanceLogLogger updatePerformanceLogLogger() {
        return UpdatePerformanceLogLogger.Noop.INSTANCE;
    }
}
