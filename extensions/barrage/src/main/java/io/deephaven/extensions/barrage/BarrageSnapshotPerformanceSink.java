//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import java.io.IOException;

/**
 * Receives barrage snapshot performance statistics so that an integrator may record them, for example into binary logs.
 * Deephaven Community Core only publishes these statistics to the in-memory table exposed by
 * {@link BarragePerformanceLog#getSnapshotTable()}; install a {@link BarrageTableLoggers.Factory} to additionally
 * record them elsewhere.
 * <p>
 * One entry is logged per snapshot request, such as an Arrow Flight {@code DoGet}, at the time that request finishes
 * writing.
 * <p>
 * All durations are nanoseconds and {@code bytesWritten} is a count of bytes. These are the raw recorded units; the
 * in-memory table scales them to milliseconds and megabits to match its column names.
 *
 * @implNote implementations need not be thread safe; all calls to a given instance are serialized.
 */
public interface BarrageSnapshotPerformanceSink {
    /**
     * @return the default name of the table that these entries describe
     */
    static String getDefaultTableName() {
        return BarrageSnapshotPerformanceLogger.getDefaultTableName();
    }

    /**
     * Record the cost of serving a single snapshot request.
     *
     * @param tableId the identity of the snapshotted table
     * @param tableKey the barrage performance key of the snapshotted table
     * @param requestTimeEpochNanos the time at which the request was received, as nanoseconds since the epoch
     * @param queueNanos the time the request spent queued before being serviced
     * @param snapshotNanos the time spent constructing the snapshot
     * @param writeNanos the time spent writing the snapshot
     * @param bytesWritten the number of bytes written
     */
    void log(String tableId, String tableKey, long requestTimeEpochNanos, long queueNanos, long snapshotNanos,
            long writeNanos, long bytesWritten) throws IOException;

    enum Noop implements BarrageSnapshotPerformanceSink {
        INSTANCE;

        @Override
        public void log(String tableId, String tableKey, long requestTimeEpochNanos, long queueNanos,
                long snapshotNanos, long writeNanos, long bytesWritten) {

        }
    }
}
