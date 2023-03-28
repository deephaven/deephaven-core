package io.deephaven.engine.tablelogger;

import io.deephaven.engine.tablelogger.impl.memory.EngineLoggersFactoryMemoryImpl;

/**
 * Provides the factory for providing engine table loggers.
 */
public class EngineLoggers {
    private EngineLoggers() {}

    private static Factory factory = new EngineLoggersFactoryMemoryImpl();

    public static Factory get() {
        return factory;
    }

    public static void set(final Factory factory) {
        EngineLoggers.factory = factory;
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
