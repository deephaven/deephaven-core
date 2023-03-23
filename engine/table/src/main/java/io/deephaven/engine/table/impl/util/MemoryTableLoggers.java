/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.clock.Clock;
import io.deephaven.configuration.Configuration;
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

    private static final int DEFAULT_PROCESSS_INFO_LOG_SIZE = Configuration.getInstance().getIntegerWithDefault(
            "defaultProcessInfoLogSize", 400);

    private volatile static MemoryTableLoggers INSTANCE;

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

    private final ProcessInfo processInfo;
    private final QueryPerformanceLogLogger qplLogger;
    private final QueryOperationPerformanceLogLogger qoplLogger;
    private final ProcessInfoLogLogger processInfoLogger;
    private final ProcessMetricsLogLogger processMetricsLogger;
    private final StatsIntradayLogger statsLogger;

    private MemoryTableLoggers() {
        final Configuration configuration = Configuration.getInstance();
        final Logger log = LoggerFactory.getLogger(MemoryTableLoggers.class);
        ProcessInfo pInfo = null;
        ProcessInfoLogLogger pInfoLogger = null;
        try {
            pInfo = ProcessInfoConfig.createForCurrentProcess(configuration);
            pInfoLogger = new ProcessInfoLogLogger(DEFAULT_PROCESSS_INFO_LOG_SIZE);
            new ProcessInfoStoreDBImpl(pInfoLogger).put(pInfo);
        } catch (IOException e) {
            log.fatal().append("Failed to configure process info: ").append(e.toString()).endl();
        }
        processInfo = pInfo;
        processInfoLogger = pInfoLogger;
        final String pInfoId = pInfo.getId().value();
        qplLogger = new QueryPerformanceLogLogger(pInfoId);
        qoplLogger = new QueryOperationPerformanceLogLogger(pInfoId);
        if (STATS_LOGGING_ENABLED) {
            processMetricsLogger = new ProcessMetricsLogLogger();
            statsLogger = new StatsIntradayLoggerDBImpl(pInfo.getId(), processMetricsLogger);
        } else {
            processMetricsLogger = null;
            statsLogger = null;
        }
    }

    public ProcessInfo getProcessInfo() {
        return processInfo;
    }

    public QueryTable getQplLoggerQueryTable() {
        return qplLogger.getQueryTable();
    }

    public QueryTable getQoplLoggerQueryTable() {
        return qoplLogger.getQueryTable();
    }

    public QueryPerformanceLogLogger getQplLogger() {
        return qplLogger;
    }

    public QueryOperationPerformanceLogLogger getQoplLogger() {
        return qoplLogger;
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
