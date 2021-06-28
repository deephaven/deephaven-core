package io.deephaven.api.agg;

import io.deephaven.api.JoinAddition;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Med implements Aggregation {

    public static Med of(JoinAddition addition) {
        return ImmutableMed.builder().addition(addition).build();
    }

    public static Med of(String x) {
        return of(JoinAddition.parse(x));
    }

    public abstract JoinAddition addition();

    @Default
    public boolean averageMedian() {
        return true;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
