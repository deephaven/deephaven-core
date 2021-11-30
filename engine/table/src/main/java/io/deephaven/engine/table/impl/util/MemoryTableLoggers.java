package io.deephaven.engine.table.impl.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.tablelogger.ProcessInfoLogLogger;
import io.deephaven.engine.tablelogger.ProcessMetricsLogLogger;
import io.deephaven.engine.tablelogger.QueryOperationPerformanceLogLogger;
import io.deephaven.engine.tablelogger.QueryPerformanceLogLogger;
import io.deephaven.io.logger.Logger;
import io.deephaven.stats.StatsIntradayLogger;
import io.deephaven.util.clock.RealTimeClock;
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
    private final MemoryTableLogger<QueryPerformanceLogLogger> qplLogger;
    private final MemoryTableLogger<QueryOperationPerformanceLogLogger> qoplLogger;
    private final MemoryTableLogger<ProcessInfoLogLogger> processInfoLogger;
    private final MemoryTableLogger<ProcessMetricsLogLogger> processMetricsLogger;
    private final StatsIntradayLogger statsLogger;

    private MemoryTableLoggers() {
        final Configuration configuration = Configuration.getInstance();
        final Logger log = LoggerFactory.getLogger(MemoryTableLoggers.class);
        ProcessInfo pInfo = null;
        MemoryTableLogger<ProcessInfoLogLogger> pInfoLogger = null;
        try {
            pInfo = ProcessInfoConfig.createForCurrentProcess(configuration);
            pInfoLogger = new MemoryTableLogger<>(
                    log, new ProcessInfoLogLogger(), ProcessInfoLogLogger.getTableDefinition(),
                    DEFAULT_PROCESSS_INFO_LOG_SIZE);
            new ProcessInfoStoreDBImpl(pInfoLogger.getTableLogger()).put(pInfo);
        } catch (IOException e) {
            log.fatal().append("Failed to configure process info: ").append(e.toString()).endl();
        }
        processInfo = pInfo;
        processInfoLogger = pInfoLogger;
        final String pInfoId = pInfo.getId().value();
        qplLogger = new MemoryTableLogger<>(
                log, new QueryPerformanceLogLogger(pInfoId), QueryPerformanceLogLogger.getTableDefinition());
        qoplLogger = new MemoryTableLogger<>(
                log, new QueryOperationPerformanceLogLogger(pInfoId),
                QueryOperationPerformanceLogLogger.getTableDefinition());
        if (STATS_LOGGING_ENABLED) {
            processMetricsLogger = new MemoryTableLogger<>(
                    log, new ProcessMetricsLogLogger(), ProcessMetricsLogLogger.getTableDefinition());
            statsLogger = new StatsIntradayLoggerDBImpl(pInfo.getId(), processMetricsLogger.getTableLogger());
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
        Driver.start(new RealTimeClock(), MemoryTableLoggers.getInstance().getStatsLogger(), fdStatsLoggingEnabled);
        return true;
    }
}
