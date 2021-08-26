/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

/**
 * Factory for iterative average aggregations.
 */
public class AvgStateFactory extends IterativeOperatorStateFactory {
    public AvgStateFactory() {}

    @Override
    public IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
        boolean exposeInternalColumns) {
        return getAvgChunked(type, name, exposeInternalColumns);
    }

    private static final AggregationMemoKey AVG_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return AVG_INSTANCE;
    }


    @Override
    boolean supportsRollup() {
        return true;
    }

    @Override
    ReaggregatableStatefactory forRollup() {
        return this;
    }

    @Override
    ReaggregatableStatefactory rollupFactory() {
        return this;
    }

    @Override
    public String toString() {
        return "Avg";
    }
}
