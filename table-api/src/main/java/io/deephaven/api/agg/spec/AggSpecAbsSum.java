package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.agg.spec.ImmutableAggSpecAbsSum;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class AggSpecAbsSum extends AggSpecEmptyBase {

    public static AggSpecAbsSum of() {
        return ImmutableAggSpecAbsSum.of();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
