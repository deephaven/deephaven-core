/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

/**
 * Factory for iterative standard deviation aggregations.
 */
public class StdSpec extends IterativeOperatorSpec {
    public StdSpec() {}

    @Override
    public IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
            boolean exposeInternalColumns) {
        return getVarChunked(type, true, name, exposeInternalColumns);
    }


    private static final AggregationMemoKey STD_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return STD_INSTANCE;
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
        return "Std";
    }
}
