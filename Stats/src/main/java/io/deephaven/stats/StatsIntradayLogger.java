//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stats;

import io.deephaven.base.stats.Counter;
import io.deephaven.base.stats.HistogramState;
import io.deephaven.base.stats.State;

public interface StatsIntradayLogger {
    /**
     * Convert a type tag to a friendly representation.
     * 
     * @param typeTag the type tag as passed to the log method
     * @return a String representation suitable for log presentation
     */
    static String type(char typeTag) {
        switch (typeTag) {
            case Counter.TYPE_TAG:
                return "counter";
            case HistogramState.TYPE_TAG:
                return "histogram";
            case State.TYPE_TAG:
                return "state";
        }
        throw new IllegalArgumentException("Unexpected typeTag: " + typeTag);
    }

    void log(String intervalName, long now, long appNow, char typeTag, String compactName, long n, long sum, long last,
            long min, long max, long avg, long sum2, long stdev);

    StatsIntradayLogger NULL = new Null();

    class Null implements StatsIntradayLogger {
        @Override
        public void log(String intervalName, long now, long appNow, char typeTag, String compactName, long n, long sum,
                long last, long min, long max, long avg, long sum2, long stdev) {}
    }
}
