package io.deephaven.api.agg;

import io.deephaven.api.JoinMatch;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Med implements Aggregation {

    public static Med of(JoinMatch match) {
        return ImmutableMed.of(match);
    }

    public static Med of(String x) {
        return of(JoinMatch.parse(x));
    }

    public abstract JoinMatch match();

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
