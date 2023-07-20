/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage;

import io.deephaven.extensions.barrage.BarragePerformanceLog.SnapshotMetricsHelper;

public interface BarrageSnapshotPerformanceLogger {
    static String getDefaultTableName() {
        return "BarrageSnapshotPerformanceLog";
    }

    void log(SnapshotMetricsHelper helper, long writeNanos, long bytesWritten);
}
