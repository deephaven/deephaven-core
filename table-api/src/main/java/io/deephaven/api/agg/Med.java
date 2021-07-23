package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class Med implements Aggregation {

    public static Med of(Pair pair) {
        return ImmutableMed.builder().pair(pair).build();
    }

    public static Med of(String x) {
        return of(Pair.parse(x));
    }

    public abstract Pair pair();

    @Default
    public boolean averageMedian() {
        return true;
    }

    public final Med withoutAverage() {
        return ImmutableMed.builder().pair(pair()).averageMedian(false).build();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
