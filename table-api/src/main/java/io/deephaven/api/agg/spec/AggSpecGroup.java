package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class AggSpecGroup extends AggSpecEmptyBase {

    public static AggSpecGroup of() {
        return ImmutableAggSpecGroup.of();
    }

    @Override
    public final String description() {
        return "group";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
