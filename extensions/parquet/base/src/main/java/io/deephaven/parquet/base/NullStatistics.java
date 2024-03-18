//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.io.api.Binary;

/**
 * A lightweight statistics object that does nothing. This should be passed to BulkWriters when we don't want to track
 * statistics.
 */
public class NullStatistics extends IntStatistics {
    public static final NullStatistics INSTANCE = new NullStatistics();

    public void updateStats(int value) {}

    public void updateStats(long value) {}

    public void updateStats(float value) {}

    public void updateStats(double value) {}

    public void updateStats(boolean value) {}

    public void updateStats(Binary value) {}

    @Override
    public void incrementNumNulls() {}

    @Override
    public void incrementNumNulls(long increment) {}

    @Override
    public String toString() {
        return "NullStatistic";
    }
}
