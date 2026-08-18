//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import org.HdrHistogram.Histogram;

import java.io.IOException;

/**
 * Receives barrage subscription performance statistics so that an integrator may record them, for example into binary
 * logs. Deephaven Community Core only publishes these statistics to the in-memory table exposed by
 * {@link BarragePerformanceLog#getSubscriptionTable()}; install a {@link BarrageTableLoggers.Factory} to additionally
 * record them elsewhere.
 * <p>
 * One entry is logged per (subscription, statistic) pair per flush cycle, where the cycle length is
 * {@link BarragePerformanceLog#CYCLE_DURATION_MILLIS}. Values are accumulated in a histogram between flushes, so each
 * entry summarizes many events.
 * <p>
 * Values are recorded in units that depend on {@code statType}: nanoseconds for the duration statistics (those whose
 * names end in {@code Millis}, such as {@code "WriteMillis"}) and bits for {@code "WriteMegabits"}. These are the raw
 * recorded units; the in-memory table scales them to milliseconds and megabits to match its column names.
 *
 * @implNote implementations need not be thread safe; all calls to a given instance are serialized.
 */
public interface BarrageSubscriptionPerformanceSink {
    /**
     * @return the default name of the table that these entries describe
     */
    static String getDefaultTableName() {
        return BarrageSubscriptionPerformanceLogger.getDefaultTableName();
    }

    /**
     * Record the statistics accumulated for a single (subscription, statistic) pair over one flush cycle.
     * <p>
     * {@code hist} is owned by the caller and is {@link Histogram#reset() reset} and reused for the next cycle as soon
     * as this method returns; an implementation must extract everything it needs before returning, and must not retain
     * a reference to it. {@link Histogram#copy() Copy} it if the values are needed later.
     *
     * @param tableId the identity of the subscribed table
     * @param tableKey the barrage performance key of the subscribed table
     * @param statType which statistic this entry describes, for example {@code "WriteMillis"}
     * @param timestampEpochNanos the time at which this cycle was flushed, as nanoseconds since the epoch
     * @param hist the values recorded during this cycle
     */
    void log(String tableId, String tableKey, String statType, long timestampEpochNanos, Histogram hist)
            throws IOException;

    enum Noop implements BarrageSubscriptionPerformanceSink {
        INSTANCE;

        @Override
        public void log(String tableId, String tableKey, String statType, long timestampEpochNanos, Histogram hist) {

        }
    }
}
