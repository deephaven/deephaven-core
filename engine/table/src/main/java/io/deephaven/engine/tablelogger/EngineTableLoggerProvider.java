package io.deephaven.engine.tablelogger;

import io.deephaven.engine.tablelogger.impl.memory.EngineTableLoggerMemoryImplFactory;

/**
 * Provides the factory for providing engine table loggers.
 */
public class EngineTableLoggerProvider {
    private EngineTableLoggerProvider() {
        throw new UnsupportedOperationException();
    }

    private static Factory factory = new EngineTableLoggerMemoryImplFactory();

    public static Factory get() {
        return factory;
    }

    public static void set(final Factory factory) {
        EngineTableLoggerProvider.factory = factory;
    }

    /**
     * Marker interface to associate engine table loggers with one another.
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
