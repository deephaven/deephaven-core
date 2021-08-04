package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class CountDistinct implements Aggregation {

    public static CountDistinct of(Pair pair) {
        return ImmutableCountDistinct.builder().pair(pair).build();
    }

    public static CountDistinct of(String x) {
        return of(Pair.parse(x));
    }

    public abstract Pair pair();

    @Default
    public boolean countNulls() {
        return false;
    }

    public final CountDistinct withNulls() {
        return ImmutableCountDistinct.builder().pair(pair()).countNulls(true).build();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
