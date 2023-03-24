package io.deephaven.engine.tablelogger;

public class EngineTableLoggerProvider {
    private EngineTableLoggerProvider() {
        throw new UnsupportedOperationException();
    }

    private static EngineTableLoggerProvider.Interface engineTableLoggerProvider = new EngineTableLoggerProviderMemoryImpl();

    public static EngineTableLoggerProvider.Interface getEngineTableLoggerProvider() {
        return engineTableLoggerProvider;
    }

    public static void setEngineTableLoggerProvider(EngineTableLoggerProvider.Interface engineTableLoggerProvider) {
        EngineTableLoggerProvider.engineTableLoggerProvider = engineTableLoggerProvider;
    }

    public interface Interface {
        ProcessInfoLogLogger processInfoLogLogger();

        ProcessMetricsLogLogger processMetricsLogLogger();

        QueryOperationPerformanceLogLogger queryOperationPerformanceLogLogger();

        QueryPerformanceLogLogger queryPerformanceLogLogger();

        ServerStateLog serverStateLog();

        UpdatePerformanceLogLogger updatePerformanceLogLogger();
    }
}
