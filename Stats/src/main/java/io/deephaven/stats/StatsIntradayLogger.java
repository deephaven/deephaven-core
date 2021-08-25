/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.stats;

public interface StatsIntradayLogger {
    void log(String intervalName, long now, long appNow, char typeTag, String compactName, long n,
        long sum, long last, long min, long max, long avg, long sum2, long stdev);

    void log(String intervalName, long now, long appNow, char typeTag, String compactName, long n,
        long sum, long last, long min, long max, long avg, long sum2, long stdev, long[] h);

    StatsIntradayLogger NULL = new Null();

    class Null implements StatsIntradayLogger {
        @Override
        public void log(String intervalName, long now, long appNow, char typeTag,
            String compactName, long n, long sum, long last, long min, long max, long avg,
            long sum2, long stdev) {}

        @Override
        public void log(String intervalName, long now, long appNow, char typeTag,
            String compactName, long n, long sum, long last, long min, long max, long avg,
            long sum2, long stdev, long[] h) {}
    }
}
