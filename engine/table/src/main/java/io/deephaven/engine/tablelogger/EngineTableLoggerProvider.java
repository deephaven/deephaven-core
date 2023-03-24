package io.deephaven.engine.tablelogger;

public class EngineTableLoggerProvider {
    private EngineTableLoggerProvider() {
        throw new UnsupportedOperationException();
    }

    private static Factory engineTableLoggerProvider = new EngineTableLoggerProviderMemoryImpl();

    public static Factory getEngineTableLoggerProvider() {
        return engineTableLoggerProvider;
    }

    public static void setEngineTableLoggerProvider(Factory engineTableLoggerProvider) {
        EngineTableLoggerProvider.engineTableLoggerProvider = engineTableLoggerProvider;
    }

    public interface Factory {
        ProcessInfoLogLogger processInfoLogLogger();

        ProcessMetricsLogLogger processMetricsLogLogger();

        QueryOperationPerformanceLogLogger queryOperationPerformanceLogLogger();

        QueryPerformanceLogLogger queryPerformanceLogLogger();

        ServerStateLog serverStateLog();

        UpdatePerformanceLogLogger updatePerformanceLogLogger();
    }
}
