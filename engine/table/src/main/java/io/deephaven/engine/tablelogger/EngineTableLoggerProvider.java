package io.deephaven.engine.tablelogger;

import io.deephaven.engine.tablelogger.impl.memory.EngineTableLoggerProviderMemoryImpl;

public class EngineTableLoggerProvider {
    private EngineTableLoggerProvider() {
        throw new UnsupportedOperationException();
    }

    private static Factory engineTableLoggerProvider = new EngineTableLoggerProviderMemoryImpl();

    public static Factory get() {
        return engineTableLoggerProvider;
    }

    public static void setEngineTableLoggerProvider(Factory engineTableLoggerProvider) {
        EngineTableLoggerProvider.engineTableLoggerProvider = engineTableLoggerProvider;
    }

    /**
     * Marker interface to associate internal table loggers with one another.
     */
    public interface EngineTableLogger {}

    public interface Factory {
        ProcessInfoLogLogger processInfoLogLogger();

        ProcessMetricsLogLogger processMetricsLogLogger();

        QueryOperationPerformanceLogLogger queryOperationPerformanceLogLogger();

        QueryPerformanceLogLogger queryPerformanceLogLogger();

        ServerStateLogLogger serverStateLog();

        UpdatePerformanceLogLogger updatePerformanceLogLogger();
    }
}
