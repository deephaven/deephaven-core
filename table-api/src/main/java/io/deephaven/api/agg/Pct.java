package io.deephaven.api.agg;

import io.deephaven.api.JoinAddition;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Pct implements Aggregation {

    public static Pct of(double percentile, JoinAddition addition) {
        return ImmutablePct.builder().addition(addition).percentile(percentile).build();
    }

    public abstract JoinAddition addition();

    public abstract double percentile();

    @Default
    public boolean averageMedian() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
