package io.deephaven.engine.tablelogger;

import io.deephaven.engine.tablelogger.impl.memory.EngineTableLoggersFactoryMemoryImpl;

/**
 * Provides the factory for providing engine table loggers.
 */
public class EngineTableLoggers {
    private EngineTableLoggers() {}

    private static Factory factory = new EngineTableLoggersFactoryMemoryImpl();

    public static Factory get() {
        return factory;
    }

    public static void set(final Factory factory) {
        EngineTableLoggers.factory = factory;
    }

    public interface Factory {
        ProcessInfoLogLogger processInfoLogLogger();

        ProcessMetricsLogLogger processMetricsLogLogger();

        QueryOperationPerformanceLogLogger queryOperationPerformanceLogLogger();

        QueryPerformanceLogLogger queryPerformanceLogLogger();

        ServerStateLogLogger serverStateLogLogger();

        UpdatePerformanceLogLogger updatePerformanceLogLogger();
    }
}
