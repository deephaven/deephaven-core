package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class AggSpecCountDistinct extends AggSpecBase {

    public static AggSpecCountDistinct of() {
        return ImmutableAggSpecCountDistinct.builder().build();
    }

    public static AggSpecCountDistinct of(boolean countNulls) {
        return ImmutableAggSpecCountDistinct.builder().countNulls(countNulls).build();
    }

    @Default
    public boolean countNulls() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
