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
        ProcessInfoLogLogger processInfoLogLogger(final int initialSizeArg);

        ProcessMetricsLogLogger processMetricsLogLogger();

        QueryOperationPerformanceLogLogger queryOperationPerformanceLogLogger(final String processUniqueId);

        QueryPerformanceLogLogger queryPerformanceLogLogger(final String processUniqueId);

        ServerStateLog serverStateLog();

        UpdatePerformanceLogLogger updatePerformanceLogLogger();
    }
}
