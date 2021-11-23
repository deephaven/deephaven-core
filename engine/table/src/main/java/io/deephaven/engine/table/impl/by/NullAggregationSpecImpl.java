package io.deephaven.engine.table.impl.by;

class NullAggregationSpecImpl implements AggregationSpec {
    private static final AggregationMemoKey NULL_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return NULL_INSTANCE;
    }
}
