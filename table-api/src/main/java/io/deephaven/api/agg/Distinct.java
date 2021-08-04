package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class Distinct implements Aggregation {

    public static Distinct of(Pair pair) {
        return ImmutableDistinct.builder().pair(pair).build();
    }

    public static Distinct of(String x) {
        return of(Pair.parse(x));
    }

    public abstract Pair pair();

    @Default
    public boolean includeNulls() {
        return false;
    }

    public final Distinct withNulls() {
        return ImmutableDistinct.builder().pair(pair()).includeNulls(true).build();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
