package io.deephaven.engine.tablelogger;

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
    public ServerStateLog serverStateLog() {
        return new ServerStateLogLoggerMemoryImpl();
    }

    @Override
    public UpdatePerformanceLogLogger updatePerformanceLogLogger() {
        return new UpdatePerformanceLogLoggerMemoryImpl();
    }
}
