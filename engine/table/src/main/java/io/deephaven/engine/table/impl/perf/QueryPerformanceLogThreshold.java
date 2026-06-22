//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.perf;

import io.deephaven.configuration.Configuration;

/**
 * This class encapsulates the parameters that control whether a given item (nugget or entry interval) is logged to one
 * of our three performance logs.
 */
public class QueryPerformanceLogThreshold {
    private final long minimumDurationNanos;
    private final long minimumReadCount;
    private final long minimumMetadataOperationCount;

    /**
     * Create a log threshold object for a particular kind of log update.
     * <ul>
     * <li>"" is for instrumented QueryPerformanceLog/QueryOperationPerformanceLog nuggets</li>
     * <li>"Uninstrumented" is for uninstrumented QueryPerformanceLog/QueryOperationPerformanceLog nuggets, and
     * <li>"Update" is for UpdatePerformanceLog entry intervals.</li>
     * </ul>
     *
     * @param kind kind of update to derive property names
     * @param defaultDuration default value for duration nanos
     * @param defaultReadCount default value for the minimum read count threshold
     * @param defaultMetadataOperationCount default value for the minimum metadata operation count threshold
     */
    public QueryPerformanceLogThreshold(String kind, long defaultDuration, long defaultReadCount,
            long defaultMetadataOperationCount) {
        minimumDurationNanos = Configuration.getInstance()
                .getLongWithDefault("QueryPerformance.minimum" + kind + "LogDurationNanos", defaultDuration);
        minimumReadCount = Configuration.getInstance()
                .getLongWithDefault("QueryPerformance.minimum" + kind + "LogReadCount", defaultReadCount);
        minimumMetadataOperationCount = Configuration.getInstance()
                .getLongWithDefault("QueryPerformance.minimum" + kind + "LogMetadataOperationCount",
                        defaultMetadataOperationCount);
    }

    /**
     * Create a log threshold object for a particular kind of log update. The minimum read count defaults to 1.
     * <ul>
     * <li>"" is for instrumented QueryPerformanceLog/QueryOperationPerformanceLog nuggets</li>
     * <li>"Uninstrumented" is for uninstrumented QueryPerformanceLog/QueryOperationPerformanceLog nuggets, and
     * <li>"Update" is for UpdatePerformanceLog entry intervals.</li>
     * </ul>
     *
     * @param kind kind of update to derive property names
     * @param defaultDuration default value for duration nanos
     */
    public QueryPerformanceLogThreshold(String kind, long defaultDuration) {
        this(kind, defaultDuration, 1, 1);
    }

    /**
     * The minimum duration for a QueryPerformanceNugget to be logged based on its duration (or entry interval usage for
     * the UpdatePerformanceLog). The value 0 logs everything. The value -1 will not log anything based on duration.
     */
    private long getMinimumDurationNanos() {
        return minimumDurationNanos;
    }

    /**
     * The minimum data read count for a QueryPerformanceNugget to be logged. The value 0 means that reads alone will
     * never trigger logging. The default value of 1 means any read will trigger logging.
     */
    private long getMinimumReadCount() {
        return minimumReadCount;
    }

    /**
     * The minimum metadata operation count for a QueryPerformanceNugget to be logged. The value 0 means that metadata
     * operations alone will never trigger logging. The default value of 1 means any metadata operation will trigger
     * logging.
     */
    private long getMinimumMetadataOperationCount() {
        return minimumMetadataOperationCount;
    }

    /**
     * Should this item be logged?
     *
     * @param duration the duration (or usage) of the item
     * @param dataReadCount the number of data read operations performed
     * @param metadataOperationCount the number of metadata operations performed
     * @return true if the item exceeds our logging threshold, and thus should be logged
     */
    public boolean shouldLog(final long duration, final long dataReadCount, final long metadataOperationCount) {
        if (getMinimumDurationNanos() >= 0 && duration >= getMinimumDurationNanos()) {
            return true;
        }
        if (getMinimumReadCount() > 0 && dataReadCount >= getMinimumReadCount()) {
            return true;
        }
        if (getMinimumMetadataOperationCount() > 0 && metadataOperationCount >= getMinimumMetadataOperationCount()) {
            return true;
        }
        return false;
    }
}
