//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stats;

public interface StatsIntradayLogger {
    /**
     * Convert a type tag to a friendly representation.
     * @param typeTag the type tag as passed to the log method
     * @return a String representation suitable for log presentation
     */
    static String type(char typeTag) {
        final char COUNTER_TAG = 'C'; // Counter.TYPE_TAG;
        final char HISTOGRAM_STATE_TAG = 'H'; // HistogramState.TYPE_TAG;
        final char STATE_TAG = 'S'; // State.TYPE_TAG;

        switch (typeTag) {
            case COUNTER_TAG:
                return "counter";
            case HISTOGRAM_STATE_TAG:
                return "histogram";
            case STATE_TAG:
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
