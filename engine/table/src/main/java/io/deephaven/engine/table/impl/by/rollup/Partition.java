package io.deephaven.engine.table.impl.by.rollup;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

/**
 * {@link RollupAggregation} that represents a partitioning of the input table.
 */
@Value.Immutable
@BuildableStyle
public abstract class Partition extends RollupAggregationBase {

    public static Partition of(boolean includeConstituents) {
        return ImmutablePartition.builder().includeConstituents(includeConstituents).build();
    }

    public abstract boolean includeConstituents();

    @Override
    public final <V extends RollupAggregation.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
