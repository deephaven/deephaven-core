package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class AggSpecSum extends AggSpecEmptyBase {

    public static AggSpecSum of() {
        return ImmutableAggSpecSum.of();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
