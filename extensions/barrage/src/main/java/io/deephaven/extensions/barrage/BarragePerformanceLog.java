/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.util.MemoryTableLogger;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

/**
 * Enable barrage performance metrics by setting the {@code BarragePerformanceLog.enableAll} configuration property, or
 * by adding the {@link io.deephaven.engine.table.Table#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE table key} as an
 * {@link io.deephaven.engine.table.Table#setAttribute(String, Object) attribute} to the table.
 */
public class BarragePerformanceLog {
    /**
     * If all barrage performance logging is enabled by default, then table's description is used as TableKey unless
     * overriden with the {@link io.deephaven.engine.table.Table#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE table key}
     * {@link io.deephaven.engine.table.Table#setAttribute(String, Object) attribute}.
     */
    public static final boolean ALL_PERFORMANCE_ENABLED = Configuration.getInstance().getBooleanForClassWithDefault(
            BarragePerformanceLog.class, "enableAll", true);

    /** Explicitly set this parameter to flush performance metrics more frequently. */
    public static final long CYCLE_DURATION_MILLIS = Configuration.getInstance().getLongForClassWithDefault(
            BarragePerformanceLog.class, "cycleDurationMillis", 60000);

    private static volatile BarragePerformanceLog INSTANCE;

    public static BarragePerformanceLog getInstance() {
        if (INSTANCE == null) {
            synchronized (BarragePerformanceLog.class) {
                if (INSTANCE == null) {
                    INSTANCE = new BarragePerformanceLog();
                }
            }
        }
        return INSTANCE;
    }

    private final MemoryTableLogger<BarragePerformanceLogLogger> barragePerformanceLogger;

    private BarragePerformanceLog() {
        final Logger log = LoggerFactory.getLogger(BarragePerformanceLog.class);
        barragePerformanceLogger = new MemoryTableLogger<>(
                log, new BarragePerformanceLogLogger(), BarragePerformanceLogLogger.getTableDefinition());
        barragePerformanceLogger.getQueryTable().setAttribute(
                BaseTable.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE, "BarragePerformanceLog");
    }

    public QueryTable getTable() {
        return barragePerformanceLogger.getQueryTable();
    }

    public BarragePerformanceLogLogger getLogger() {
        return barragePerformanceLogger.getTableLogger();
    }
}
