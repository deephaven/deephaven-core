package io.deephaven.engine.table.impl.by.rollup;

import io.deephaven.api.agg.Aggregation;

public abstract class RollupAggregationBase implements RollupAggregation {

    @Override
    public final <V extends Aggregation.Visitor> V walk(V visitor) {
        if (visitor instanceof Visitor) {
            walk((Visitor) visitor);
            return visitor;
        }
        throw new UnsupportedOperationException("Cannot walk a RollupAggregation without a RollupAggregation.Visitor");
    }
}
