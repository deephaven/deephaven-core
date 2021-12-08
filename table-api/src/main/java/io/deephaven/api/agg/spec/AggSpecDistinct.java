package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class AggSpecDistinct extends AggSpecBase {

    public static AggSpecDistinct of() {
        return ImmutableAggSpecDistinct.builder().build();
    }

    public static AggSpecDistinct of(boolean includeNulls) {
        return ImmutableAggSpecDistinct.builder().includeNulls(includeNulls).build();
    }

    @Default
    public boolean includeNulls() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
