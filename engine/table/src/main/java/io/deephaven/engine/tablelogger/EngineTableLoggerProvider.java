package io.deephaven.engine.tablelogger;

import io.deephaven.engine.tablelogger.impl.memory.EngineTableLoggerMemoryImplFactory;

public class EngineTableLoggerProvider {
    private EngineTableLoggerProvider() {
        throw new UnsupportedOperationException();
    }

    private static Factory factory = new EngineTableLoggerMemoryImplFactory();

    public static Factory get() {
        return factory;
    }

    public static void set(Factory factory) {
        EngineTableLoggerProvider.factory = factory;
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

        ServerStateLogLogger serverStateLogLogger();

        UpdatePerformanceLogLogger updatePerformanceLogLogger();
    }
}
