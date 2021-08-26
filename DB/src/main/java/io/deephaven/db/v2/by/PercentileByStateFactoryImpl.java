/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Require;

import java.io.Serializable;
import java.util.Objects;

public class PercentileByStateFactoryImpl implements Serializable, AggregationStateFactory {
    private final double percentile;
    private final boolean averageMedian;

    public PercentileByStateFactoryImpl(double percentile) {
        this(percentile, false);
    }

    public PercentileByStateFactoryImpl(double percentile, boolean averageMedian) {
        this.percentile = percentile;
        this.averageMedian = averageMedian;
        Require.inRange(percentile, 0.0, 1.0, "percentile");
    }

    private static class MemoKey implements AggregationMemoKey {
        private final double percentile;
        private final boolean averageMedian;

        private MemoKey(double percentile, boolean averageMedian) {
            this.percentile = percentile;
            this.averageMedian = averageMedian;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final MemoKey memoKey = (MemoKey) o;
            return Double.compare(memoKey.percentile, percentile) == 0 &&
                    averageMedian == memoKey.averageMedian;
        }

        @Override
        public int hashCode() {
            return Objects.hash(percentile, averageMedian);
        }
    }

    public double getPercentile() {
        return percentile;
    }

    public boolean getAverageMedian() {
        return averageMedian;
    }

    @Override
    public AggregationMemoKey getMemoKey() {
        return new MemoKey(percentile, averageMedian);
    }
}
