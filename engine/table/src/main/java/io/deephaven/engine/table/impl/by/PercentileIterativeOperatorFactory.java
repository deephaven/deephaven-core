/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

/**
 * Factory for iterative sum aggregations.
 */
public class PercentileIterativeOperatorFactory implements IterativeChunkedOperatorFactory {
    private final boolean averageMedian;
    private final double percentile;

    public PercentileIterativeOperatorFactory(double percentile, boolean averageMedian) {
        this.averageMedian = averageMedian;
        this.percentile = percentile;
    }

    @Override
    public IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
            boolean exposeInternalColumns) {
        return IterativeOperatorSpec.getPercentileChunked(type, name, percentile, averageMedian);
    }

    @Override
    public String toString() {
        return averageMedian ? "Median" : "Percentile(" + percentile + ")";
    }
}
