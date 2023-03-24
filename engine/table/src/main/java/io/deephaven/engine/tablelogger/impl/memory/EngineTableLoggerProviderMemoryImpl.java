package io.deephaven.engine.tablelogger.impl.memory;

import io.deephaven.engine.tablelogger.EngineTableLoggerProvider;
import io.deephaven.engine.tablelogger.ProcessInfoLogLogger;
import io.deephaven.engine.tablelogger.ProcessMetricsLogLogger;
import io.deephaven.engine.tablelogger.QueryOperationPerformanceLogLogger;
import io.deephaven.engine.tablelogger.QueryPerformanceLogLogger;
import io.deephaven.engine.tablelogger.ServerStateLogLogger;
import io.deephaven.engine.tablelogger.UpdatePerformanceLogLogger;

public class EngineTableLoggerProviderMemoryImpl implements EngineTableLoggerProvider.Factory {
    @Override
    public ProcessInfoLogLogger processInfoLogLogger() {
        return new ProcessInfoLogLoggerMemoryImpl();
    }

    @Override
    public ProcessMetricsLogLogger processMetricsLogLogger() {
        return new ProcessMetricsLogLoggerMemoryImpl();
    }

    @Override
    public QueryOperationPerformanceLogLogger queryOperationPerformanceLogLogger() {
        return new QueryOperationPerformanceLogLoggerMemoryImpl();
    }

    @Override
    public QueryPerformanceLogLogger queryPerformanceLogLogger() {
        return new QueryPerformanceLogLoggerMemoryImpl();
    }

    @Override
    public ServerStateLogLogger serverStateLog() {
        return new ServerStateLogLoggerMemoryImpl();
    }

    @Override
    public UpdatePerformanceLogLogger updatePerformanceLogLogger() {
        return new UpdatePerformanceLogLoggerMemoryImpl();
    }
}
