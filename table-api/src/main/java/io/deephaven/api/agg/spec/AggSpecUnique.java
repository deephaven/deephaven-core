package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class AggSpecUnique extends AggSpecBase {

    public static AggSpecUnique of() {
        return ImmutableAggSpecUnique.builder().build();
    }

    public static AggSpecUnique of(boolean includeNulls) {
        return ImmutableAggSpecUnique.builder().includeNulls(includeNulls).build();
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
