//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import org.HdrHistogram.Histogram;

import java.time.Instant;

public interface BarrageSubscriptionPerformanceLogger {
    static String getDefaultTableName() {
        return "BarrageSubscriptionPerformanceLog";
    }

    void log(String tableId, String tableKey, String statType, Instant now, Histogram hist);
}
