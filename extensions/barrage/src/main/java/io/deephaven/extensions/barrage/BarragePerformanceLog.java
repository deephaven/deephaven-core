//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.AttributeMap;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Enable barrage performance metrics by setting the {@code BarragePerformanceLog.enableAll} configuration property, or
 * by adding the {@link io.deephaven.engine.table.Table#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE table key} as an
 * {@link Table#withAttributes(Map)} attribute} to the table.
 */
public class BarragePerformanceLog {
    private static final Logger log = LoggerFactory.getLogger(BarragePerformanceLog.class);

    /**
     * If all barrage performance logging is enabled by default, then table's description is used as TableKey unless
     * overridden with the {@link io.deephaven.engine.table.Table#BARRAGE_PERFORMANCE_KEY_ATTRIBUTE table key}
     * {@link Table#withAttributes(Map)} attribute}.
     */
    public static final boolean ALL_PERFORMANCE_ENABLED = Configuration.getInstance().getBooleanForClassWithDefault(
            BarragePerformanceLog.class, "enableAll", true);

    /** Explicitly set this parameter to flush performance metrics more frequently. */
    public static final long CYCLE_DURATION_MILLIS = Configuration.getInstance().getLongForClassWithDefault(
            BarragePerformanceLog.class, "cycleDurationMillis", 60000);

    private static volatile BarragePerformanceLog INSTANCE;

    /**
     * Get the singleton instance, creating it if necessary. The instance resolves its performance sinks from
     * {@link BarrageTableLoggers#get()} when it is created, so
     * {@link BarrageTableLoggers#set(BarrageTableLoggers.Factory)} must be called before first use for an
     * integrator-provided factory to take effect.
     * <p>
     * NB: Creating the instance registers the in-memory blink tables with the update graph of the calling thread's
     * {@link io.deephaven.engine.context.ExecutionContext}, so the first call must come from a context that has a live
     * update graph. Callers should let the first real barrage activity create the instance rather than forcing it
     * during startup, when no such context typically exists yet.
     *
     * @return the singleton {@link BarragePerformanceLog}
     */
    public static BarragePerformanceLog getInstance() {
        BarragePerformanceLog local;
        if ((local = INSTANCE) == null) {
            synchronized (BarragePerformanceLog.class) {
                if ((local = INSTANCE) == null) {
                    INSTANCE = local = new BarragePerformanceLog();
                }
            }
        }
        return local;
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

    private final BarrageSubscriptionPerformanceLoggerImpl subImpl;
    private final BarrageSnapshotPerformanceLoggerImpl snapImpl;

    private BarragePerformanceLog() {
        final BarrageTableLoggers.Factory loggerFactory = BarrageTableLoggers.get();
        subImpl = new BarrageSubscriptionPerformanceLoggerImpl(resolveSink(
                loggerFactory::subscriptionPerformanceSink,
                BarrageSubscriptionPerformanceSink.Noop.INSTANCE,
                "subscription"));
        snapImpl = new BarrageSnapshotPerformanceLoggerImpl(resolveSink(
                loggerFactory::snapshotPerformanceSink,
                BarrageSnapshotPerformanceSink.Noop.INSTANCE,
                "snapshot"));
    }

    /**
     * Obtain a sink from the installed {@link BarrageTableLoggers.Factory}, falling back to {@code noop} if the factory
     * fails to provide one. A sink that cannot be created must degrade the recording of barrage performance data, not
     * barrage itself: this constructor runs on whichever thread first uses barrage, and a failure here would leave
     * {@link #INSTANCE} null so that every later {@link #getInstance()} retries and re-throws.
     */
    private static <SINK_TYPE> SINK_TYPE resolveSink(
            final Supplier<SINK_TYPE> sinkSupplier,
            final SINK_TYPE noop,
            final String description) {
        final SINK_TYPE sink;
        try {
            sink = sinkSupplier.get();
        } catch (final Exception e) {
            log.error().append("Error creating the barrage ").append(description)
                    .append(" performance sink; barrage ").append(description)
                    .append(" performance will be reported to the in-memory table only, caused by: ").append(e).endl();
            return noop;
        }
        if (sink == null) {
            log.error().append("The installed BarrageTableLoggers.Factory returned a null barrage ")
                    .append(description).append(" performance sink; barrage ").append(description)
                    .append(" performance will be reported to the in-memory table only").endl();
            return noop;
        }
        return sink;
    }

    public QueryTable getSubscriptionTable() {
        return (QueryTable) BlinkTableTools.blinkToAppendOnly(subImpl.blinkTable());
    }

    public BarrageSubscriptionPerformanceLogger getSubscriptionLogger() {
        return subImpl;
    }

    public QueryTable getSnapshotTable() {
        return (QueryTable) BlinkTableTools.blinkToAppendOnly(snapImpl.blinkTable());
    }

    public BarrageSnapshotPerformanceLogger getSnapshotLogger() {
        return snapImpl;
    }

    public static class SnapshotMetricsHelper implements BarrageMessageWriter.WriteMetricsConsumer {
        public final Instant requestTm = DateTimeUtils.now();

        public String tableId;
        public String tableKey;
        public long queueNanos;
        public long snapshotNanos;

        @Override
        public void onWrite(long bytesWritten, long writeNanos) {
            if (tableKey == null) {
                // metrics for this request are not to be reported
                return;
            }
            BarragePerformanceLog.getInstance()
                    .getSnapshotLogger()
                    .log(this, writeNanos, bytesWritten);
        }
    }
}
