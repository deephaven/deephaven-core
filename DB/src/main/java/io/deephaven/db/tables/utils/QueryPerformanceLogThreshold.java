package io.deephaven.db.tables.utils;

import io.deephaven.configuration.Configuration;

/**
 * This class encapsulates the parameters that control whether a given item (nugget or entry interval) is logged to one
 * of our three performance logs.
 */
public class QueryPerformanceLogThreshold {
    private final long minimumDurationNanos;

    /**
     * Create a log threshold object for a particular kind of log update
     * <ul>
     * <li>"" is for instrumented QueryPerformanceLog/QueryOperationPerformanceLog nuggets</li>
     * <li>"Uninstrumented" is for uninstrumented QueryPerformanceLog/QueryOperationPerformanceLog nuggets, and
     * <li>"Update" is for UpdatePerformanceLog entry intervals.</li>
     * </ul>
     *
     * @param kind kind of update to derive property names
     * @param defaultDuration default value for duration nanos
     * @param defaultRepeatedReads default value for repeated read threshold
     * @param defaultInitialReads default value for initial read threshold
     */
    private QueryPerformanceLogThreshold(String kind, long defaultDuration, long defaultRepeatedReads,
            long defaultInitialReads) {
        minimumDurationNanos = Configuration.getInstance()
                .getLongWithDefault("QueryPerformance.minimum" + kind + "LogDurationNanos", defaultDuration);
    }

    /**
     * Create a log threshold object for a particular kind of log update
     * <ul>
     * <li>"" is for instrumented QueryPerformanceLog/QueryOperationPerformanceLog nuggets</li>
     * <li>"Uninstrumented" is for uninstrumented QueryPerformanceLog/QueryOperationPerformanceLog nuggets, and
     * <li>"Update" is for UpdatePerformanceLog entry intervals.</li>
     * </ul>
     *
     * The initial and repeated read threshold defaults to 1.
     *
     * @param kind kind of update to derive property names
     * @param defaultDuration default value for duration nanos
     */
    public QueryPerformanceLogThreshold(String kind, long defaultDuration) {
        this(kind, defaultDuration, 1, 1);
    }

    /**
     * The minimum duration for an QueryPerformanceNugget to be logged based on its duration (or entry interval usage
     * for the UpdatePerformanceLog). The value 0 logs everything. The value -1 will not log anything based on duration.
     */
    private long getMinimumDurationNanos() {
        return minimumDurationNanos;
    }

    /**
     * Should this item be logged?
     *
     * @param duration the duration (or usage) of the item
     * @return true if the item exceeds our logging threshold, and thus should be logged
     */
    public boolean shouldLog(final long duration) {
        if (getMinimumDurationNanos() >= 0 && duration >= getMinimumDurationNanos()) {
            return true;
        }
        return false;
    }
}
