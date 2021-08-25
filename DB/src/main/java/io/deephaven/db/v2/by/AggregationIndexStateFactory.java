/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

public class AggregationIndexStateFactory implements AggregationStateFactory {
    private static final AggregationMemoKey AGGREGATION_INDEX_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return AGGREGATION_INDEX_INSTANCE;
    }
}
