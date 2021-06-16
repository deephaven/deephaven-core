package io.deephaven.qst.table.agg;

import io.deephaven.qst.table.JoinMatch;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
public abstract class Pct implements Aggregation {

    public static Pct of(JoinMatch match, double pct) {
        return ImmutablePct.of(match, pct);
    }

    public abstract JoinMatch match();

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
