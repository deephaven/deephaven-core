package io.deephaven.engine.tablelogger;

public class EngineTableLoggerProviderMemoryImpl implements EngineTableLoggerProvider.Interface {
    @Override
    public ProcessInfoLogLogger processInfoLogLogger(final int initialSizeArg) {
        return new ProcessInfoLogLoggerMemoryImpl(initialSizeArg);
    }

    @Override
    public ProcessMetricsLogLogger processMetricsLogLogger() {
        return new ProcessMetricsLogLoggerMemoryImpl();
    }

    @Override
    public QueryOperationPerformanceLogLogger queryOperationPerformanceLogLogger(final String processUniqueId) {
        return new QueryOperationPerformanceLogLoggerMemoryImpl(processUniqueId);
    }

    @Override
    public QueryPerformanceLogLogger queryPerformanceLogLogger(final String processUniqueId) {
        return new QueryPerformanceLogLoggerMemoryImpl(processUniqueId);
    }

    @Override
    public ServerStateLog serverStateLog() {
        return new ServerStateLogLoggerMemoryImpl();
    }

    @Override
    public UpdatePerformanceLogLogger updatePerformanceLogLogger() {
        return null;
    }
}
