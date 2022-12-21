/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.AttributeMap;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.util.MemoryTableLogger;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Enable barrage performance metrics by setting the {@code BarragePerformanceLog.enableAll} configuration property, or
 * by adding the {@link io.deephaven.engine.table.Table#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE table key} as an
 * {@link io.deephaven.engine.table.Table#setAttribute(String, Object) attribute} to the table.
 */
public class BarragePerformanceLog {
    /**
     * If all barrage performance logging is enabled by default, then table's description is used as TableKey unless
     * overridden with the {@link io.deephaven.engine.table.Table#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE table key}
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

    /**
     * Return the performance key for the provided table.
     *
     * @param table the table that will be logged
     * @return the table key or null if no entry should not be logged
     */
    public static String getKeyFor(@NotNull final Table table) {
        return getKeyFor(table, table::getDescription);
    }

    /**
     * Return the performance key for the provided source.
     *
     * @param source the source that will be logged
     * @param descriptionSupplier supplier for a description of the source
     * @return the table key or null if no entry should not be logged
     */
    @Nullable
    public static String getKeyFor(
            @NotNull final AttributeMap source,
            @NotNull final Supplier<String> descriptionSupplier) {
        final Object statsKey = source.getAttribute(Table.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE);

        if (statsKey instanceof String) {
            return (String) statsKey;
        } else if (BarragePerformanceLog.ALL_PERFORMANCE_ENABLED) {
            return descriptionSupplier.get();
        }

        return null;
    }

    private final MemoryTableLogger<BarrageSubscriptionPerformanceLogger> barrageSubscriptionPerformanceLogger;
    private final MemoryTableLogger<BarrageSnapshotPerformanceLogger> barrageSnapshotPerformanceLogger;

    private BarragePerformanceLog() {
        final Logger log = LoggerFactory.getLogger(BarragePerformanceLog.class);
        barrageSubscriptionPerformanceLogger = new MemoryTableLogger<>(
                log, new BarrageSubscriptionPerformanceLogger(),
                BarrageSubscriptionPerformanceLogger.getTableDefinition());
        barrageSubscriptionPerformanceLogger.getQueryTable().setAttribute(
                BaseTable.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE,
                BarrageSubscriptionPerformanceLogger.getDefaultTableName());
        barrageSnapshotPerformanceLogger = new MemoryTableLogger<>(
                log, new BarrageSnapshotPerformanceLogger(), BarrageSnapshotPerformanceLogger.getTableDefinition());
        barrageSnapshotPerformanceLogger.getQueryTable().setAttribute(
                BaseTable.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE, BarrageSnapshotPerformanceLogger.getDefaultTableName());
    }

    public QueryTable getSubscriptionTable() {
        return barrageSubscriptionPerformanceLogger.getQueryTable();
    }

    public BarrageSubscriptionPerformanceLogger getSubscriptionLogger() {
        return barrageSubscriptionPerformanceLogger.getTableLogger();
    }

    public QueryTable getSnapshotTable() {
        return barrageSnapshotPerformanceLogger.getQueryTable();
    }

    public BarrageSnapshotPerformanceLogger getSnapshotLogger() {
        return barrageSnapshotPerformanceLogger.getTableLogger();
    }

    public interface WriteMetricsConsumer {
        void onWrite(long bytes, long cpuNanos);
    }

    public static class SnapshotMetricsHelper implements WriteMetricsConsumer {
        private final DateTime requestTm = DateTime.now();
        public String tableId;
        public String tableKey;
        public long queueNanos;
        public long snapshotNanos;

        public void onWrite(long bytesWritten, long writeNanos) {
            if (tableKey == null) {
                // metrics for this request are not to be reported
                return;
            }

            try {
                BarragePerformanceLog.getInstance().getSnapshotLogger()
                        .log(tableId, tableKey, requestTm, queueNanos, snapshotNanos, writeNanos, bytesWritten);
            } catch (IOException ignored) {
            }
        }
    }
}
