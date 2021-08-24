/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

/**
 * Factory for iterative variance aggregations.
 */
public class VarStateFactory extends IterativeOperatorStateFactory {
    public VarStateFactory() {}

    @Override
    public IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
        boolean exposeInternalColumns) {
        return getVarChunked(type, false, name, exposeInternalColumns);
    }

    private static final AggregationMemoKey VAR_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return VAR_INSTANCE;
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
        return "Var";
    }
}
