package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class AggSpecMax extends AggSpecEmptyBase {

    public static AggSpecMax of() {
        return ImmutableAggSpecMax.of();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
