/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.clock.Clock;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.tablelogger.EngineTableLoggers;
import io.deephaven.engine.tablelogger.QueryOperationPerformanceLogLogger;
import io.deephaven.engine.tablelogger.QueryPerformanceLogLogger;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.process.ProcessInfo;
import io.deephaven.process.ProcessInfoConfig;
import io.deephaven.stats.Driver;
import io.deephaven.stats.StatsIntradayLogger;

import java.io.IOException;

public class EngineMetrics {
    private static final boolean STATS_LOGGING_ENABLED = Configuration.getInstance().getBooleanWithDefault(
            "statsLoggingEnabled", true);
    private static volatile ProcessInfo PROCESS_INFO;
    private static volatile EngineMetrics ENGINE_METRICS;

    public static ProcessInfo getProcessInfo() {
        ProcessInfo local;
        if ((local = PROCESS_INFO) == null) {
            synchronized (EngineMetrics.class) {
                if ((local = PROCESS_INFO) == null) {
                    try {
                        PROCESS_INFO = local = ProcessInfoConfig.createForCurrentProcess(Configuration.getInstance());
                    } catch (IOException e) {
                        throw new IllegalStateException("Failed to create process info.", e);
                    }
                }
            }
        }
        return local;
    }

    public static EngineMetrics getInstance() {
        EngineMetrics local;
        if ((local = ENGINE_METRICS) == null) {
            synchronized (EngineMetrics.class) {
                if ((local = ENGINE_METRICS) == null) {
                    ENGINE_METRICS = local = new EngineMetrics();
                }
            }
        }
        return local;
    }

    private final QueryPerformanceImpl qpImpl;
    private final QueryOperationPerformanceImpl qoplImpl;
    private final ProcessInfoImpl processInfoImpl;
    private final StatsImpl statsImpl;

    private EngineMetrics() {
        EngineTableLoggers.Factory tableLoggerFactory = EngineTableLoggers.get();
        final Logger log = LoggerFactory.getLogger(EngineMetrics.class);
        ProcessInfo pInfo = getProcessInfo();
        processInfoImpl = new ProcessInfoImpl(pInfo.getId(), tableLoggerFactory.processInfoLogLogger());
        try {
            processInfoImpl.init(pInfo);
        } catch (IOException e) {
            log.fatal().append("Failed to configure process info: ").append(e.toString()).endl();
        }
        qpImpl = new QueryPerformanceImpl(pInfo.getId(), tableLoggerFactory.queryPerformanceLogLogger());
        qoplImpl = new QueryOperationPerformanceImpl(pInfo.getId(),
                tableLoggerFactory.queryOperationPerformanceLogLogger());
        if (STATS_LOGGING_ENABLED) {
            statsImpl = new StatsImpl(pInfo.getId(), tableLoggerFactory.processMetricsLogLogger());
        } else {
            statsImpl = null;
        }
    }

    public QueryTable getQplLoggerQueryTable() {
        return (QueryTable) BlinkTableTools.blinkToAppendOnly(qpImpl.blinkTable());
    }

    public QueryTable getQoplLoggerQueryTable() {
        return (QueryTable) BlinkTableTools.blinkToAppendOnly(qoplImpl.blinkTable());
    }

    public QueryPerformanceLogLogger getQplLogger() {
        return qpImpl;
    }

    public QueryOperationPerformanceLogLogger getQoplLogger() {
        return qoplImpl;
    }

    public QueryTable getProcessInfoQueryTable() {
        return (QueryTable) processInfoImpl.table();
    }

    public QueryTable getProcessMetricsQueryTable() {
        return statsImpl == null ? null : (QueryTable) BlinkTableTools.blinkToAppendOnly(statsImpl.blinkTable());
    }

    private StatsIntradayLogger getStatsLogger() {
        return statsImpl;
    }

    public static boolean maybeStartStatsCollection() {
        if (!EngineMetrics.STATS_LOGGING_ENABLED) {
            return false;
        }
        final boolean fdStatsLoggingEnabled = Configuration.getInstance().getBooleanWithDefault(
                "fdStatsLoggingEnabled", false);
        Driver.start(Clock.system(), EngineMetrics.getInstance().getStatsLogger(), fdStatsLoggingEnabled);
        return true;
    }
}
