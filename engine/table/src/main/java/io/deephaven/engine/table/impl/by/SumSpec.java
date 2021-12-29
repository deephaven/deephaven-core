/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

/**
 * Factory for iterative sum aggregations.
 */
public class SumSpec extends IterativeOperatorSpec {
    public SumSpec() {}

    @Override
    public IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
            boolean exposeInternalColumns) {
        return getSumChunked(type, name);
    }

    private static final AggregationMemoKey SUM_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return SUM_INSTANCE;
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
        return "Sum";
    }
}
