/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;


/**
 * Factory for iterative absolute sum aggregations.
 */
public class AbsSumStateFactory extends IterativeOperatorStateFactory {
    public AbsSumStateFactory() {}

    @Override
    public IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
            boolean exposeInternalColumns) {
        return getAbsSumChunked(type, name);
    }

    private static final AggregationMemoKey ABS_SUM_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return ABS_SUM_INSTANCE;
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
        return "AbsSum";
    }
}
