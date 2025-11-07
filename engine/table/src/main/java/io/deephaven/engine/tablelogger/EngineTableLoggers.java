//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

/**
 * Provides the factory for providing engine table loggers.
 */
public class EngineTableLoggers {
    private EngineTableLoggers() {}

    private static Factory factory = Factory.Noop.INSTANCE;

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

        UpdatePerformanceAncestorLogger updatePerformanceAncestorLogger();

        enum Noop implements Factory {
            INSTANCE;

            @Override
            public ProcessInfoLogLogger processInfoLogLogger() {
                return ProcessInfoLogLogger.Noop.INSTANCE;
            }

            @Override
            public ProcessMetricsLogLogger processMetricsLogLogger() {
                return ProcessMetricsLogLogger.Noop.INSTANCE;
            }

            @Override
            public QueryOperationPerformanceLogLogger queryOperationPerformanceLogLogger() {
                return QueryOperationPerformanceLogLogger.Noop.INSTANCE;
            }

            @Override
            public QueryPerformanceLogLogger queryPerformanceLogLogger() {
                return QueryPerformanceLogLogger.Noop.INSTANCE;
            }

            @Override
            public ServerStateLogLogger serverStateLogLogger() {
                return ServerStateLogLogger.Noop.INSTANCE;
            }

            @Override
            public UpdatePerformanceLogLogger updatePerformanceLogLogger() {
                return UpdatePerformanceLogLogger.Noop.INSTANCE;
            }

            @Override
            public UpdatePerformanceAncestorLogger updatePerformanceAncestorLogger() {
                return UpdatePerformanceAncestorLogger.Noop.INSTANCE;
            }
        }
    }
}
