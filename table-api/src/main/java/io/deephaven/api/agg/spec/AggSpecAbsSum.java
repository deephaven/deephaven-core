package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that supplies the absolute sum for each group. Only works with numeric input types.
 */
@Immutable
@SimpleStyle
public abstract class AggSpecAbsSum extends AggSpecEmptyBase {

    public static AggSpecAbsSum of() {
        return ImmutableAggSpecAbsSum.of();
    }

    @Override
    public final String description() {
        return "absolute sum";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
