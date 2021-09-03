package io.deephaven.engine.v2.by;

class NullAggregationStateFactoryImpl implements AggregationStateFactory {
    private static final AggregationMemoKey NULL_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return NULL_INSTANCE;
    }
}
