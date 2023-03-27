/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.clock.Clock;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.tablelogger.EngineTableLoggerProvider;
import io.deephaven.engine.tablelogger.ProcessInfoLogLogger;
import io.deephaven.engine.tablelogger.ProcessMetricsLogLogger;
import io.deephaven.engine.tablelogger.QueryOperationPerformanceLogLogger;
import io.deephaven.engine.tablelogger.QueryPerformanceLogLogger;
import io.deephaven.io.logger.Logger;
import io.deephaven.stats.StatsIntradayLogger;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.process.ProcessInfo;
import io.deephaven.process.ProcessInfoConfig;
import io.deephaven.process.ProcessInfoStoreDBImpl;
import io.deephaven.process.StatsIntradayLoggerDBImpl;
import io.deephaven.stats.Driver;

import java.io.IOException;

public class MemoryTableLoggers {
    private static final boolean STATS_LOGGING_ENABLED = Configuration.getInstance().getBooleanWithDefault(
            "statsLoggingEnabled", true);
    private static final ProcessInfo processInfo;
    private volatile static MemoryTableLoggers INSTANCE;

    static {
        try {
            processInfo = ProcessInfoConfig.createForCurrentProcess(Configuration.getInstance());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create process info.", e);
        }
    }

    public static ProcessInfo getProcessInfo() {
        return processInfo;
    }

    public static MemoryTableLoggers getInstance() {
        if (INSTANCE == null) {
            synchronized (MemoryTableLoggers.class) {
                if (INSTANCE == null) {
                    INSTANCE = new MemoryTableLoggers();
                }
            }
        }
        return INSTANCE;
    }

    private final MemoryTableLoggerWrapper<QueryPerformanceLogLogger> qplLogger;
    private final MemoryTableLoggerWrapper<QueryOperationPerformanceLogLogger> qoplLogger;
    private final MemoryTableLoggerWrapper<ProcessInfoLogLogger> processInfoLogger;
    private final MemoryTableLoggerWrapper<ProcessMetricsLogLogger> processMetricsLogger;
    private final StatsIntradayLogger statsLogger;

    private MemoryTableLoggers() {
        EngineTableLoggerProvider.Factory tableLoggerFactory = EngineTableLoggerProvider.get();
        final Logger log = LoggerFactory.getLogger(MemoryTableLoggers.class);
        MemoryTableLoggerWrapper<ProcessInfoLogLogger> pInfoLogger = null;
        try {
            pInfoLogger = new MemoryTableLoggerWrapper<>(tableLoggerFactory.processInfoLogLogger());
            new ProcessInfoStoreDBImpl(pInfoLogger.getTableLogger()).put(processInfo);
        } catch (IOException e) {
            log.fatal().append("Failed to configure process info: ").append(e.toString()).endl();
        }
        processInfoLogger = pInfoLogger;
        qplLogger = new MemoryTableLoggerWrapper<>(tableLoggerFactory.queryPerformanceLogLogger());
        qoplLogger = new MemoryTableLoggerWrapper<>(tableLoggerFactory.queryOperationPerformanceLogLogger());
        if (STATS_LOGGING_ENABLED) {
            processMetricsLogger = new MemoryTableLoggerWrapper<>(tableLoggerFactory.processMetricsLogLogger());
            statsLogger = new StatsIntradayLoggerDBImpl(processInfo.getId(), processMetricsLogger.getTableLogger());
        } else {
            processMetricsLogger = null;
            statsLogger = null;
        }
    }

    public QueryTable getQplLoggerQueryTable() {
        return qplLogger.getQueryTable();
    }

    public QueryTable getQoplLoggerQueryTable() {
        return qoplLogger.getQueryTable();
    }

    public QueryPerformanceLogLogger getQplLogger() {
        return qplLogger.getTableLogger();
    }

    public QueryOperationPerformanceLogLogger getQoplLogger() {
        return qoplLogger.getTableLogger();
    }

    public QueryTable getProcessInfoQueryTable() {
        return processInfoLogger.getQueryTable();
    }

    public QueryTable getProcessMetricsQueryTable() {
        if (processMetricsLogger != null) {
            return processMetricsLogger.getQueryTable();
        }
        return null;
    }

    private StatsIntradayLogger getStatsLogger() {
        return statsLogger;
    }

    public static boolean maybeStartStatsCollection() {
        if (!MemoryTableLoggers.STATS_LOGGING_ENABLED) {
            return false;
        }
        final boolean fdStatsLoggingEnabled = Configuration.getInstance().getBooleanWithDefault(
                "fdStatsLoggingEnabled", false);
        Driver.start(Clock.system(), MemoryTableLoggers.getInstance().getStatsLogger(), fdStatsLoggingEnabled);
        return true;
    }
}
