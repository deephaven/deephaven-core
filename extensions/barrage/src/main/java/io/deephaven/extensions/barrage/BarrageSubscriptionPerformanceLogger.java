//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import org.HdrHistogram.Histogram;

import java.time.Instant;

public interface BarrageSubscriptionPerformanceLogger {
    static String getDefaultTableName() {
        return "BarrageSubscriptionPerformanceLog";
    }

    /**
     * The {@code statType} values recorded by barrage itself.
     * <p>
     * Values are unscaled: durations are nanoseconds and payload sizes are bytes, matching {@code UpdatePerformanceLog}
     * and the rest of the engine's performance streams.
     * <p>
     * These are conventions rather than a closed set; an integrator may log additional statistics under names of its
     * own, and should follow the same {@code <Stem><Unit>} convention.
     */
    final class StatType {
        /** Time to record the changes that occurred during a single update graph cycle. */
        public static final String ENQUEUE_NANOS = "EnqueueNanos";
        /** Time to aggregate multiple updates within the same interval. */
        public static final String AGGREGATE_NANOS = "AggregateNanos";
        /** Time to deliver an aggregated message to all subscribers. */
        public static final String PROPAGATE_NANOS = "PropagateNanos";
        /** Time to snapshot data for a new or changed subscription. */
        public static final String SNAPSHOT_NANOS = "SnapshotNanos";
        /** Time to run one full cycle of the off-thread propagation logic. */
        public static final String UPDATE_JOB_NANOS = "UpdateJobNanos";
        /** Time to write an update to a single subscriber. */
        public static final String WRITE_NANOS = "WriteNanos";
        /** Payload size of an update written to a single subscriber, in bytes. */
        public static final String WRITE_BYTES = "WriteBytes";
        /** Time to read and deserialize an update from the wire. */
        public static final String DESERIALIZATION_NANOS = "DeserializationNanos";
        /** Time to apply a single update during the update graph cycle. */
        public static final String PROCESS_UPDATE_NANOS = "ProcessUpdateNanos";
        /** Time to apply all queued updates during a single update graph cycle. */
        public static final String REFRESH_NANOS = "RefreshNanos";

        private StatType() {}
    }

    /**
     * Publish the statistics accumulated in {@code hist} for a single (subscription, statistic) pair.
     *
     * @param tableId the identity of the subscribed table
     * @param tableKey the barrage performance key of the subscribed table
     * @param statType which statistic this entry describes; see {@link StatType}
     * @param now the time at which this cycle was flushed
     * @param hist the values recorded during this cycle, in the unit named by {@code statType}
     */
    void log(String tableId, String tableKey, String statType, Instant now, Histogram hist);
}
