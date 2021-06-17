package io.deephaven.api.agg;

import io.deephaven.api.JoinMatch;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class CountDistinct implements Aggregation {

    public static CountDistinct of(JoinMatch match) {
        return ImmutableCountDistinct.builder().match(match).build();
    }

    public static CountDistinct of(String x) {
        return of(JoinMatch.parse(x));
    }

    public abstract JoinMatch match();

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
