package io.deephaven.api.agg;

import io.deephaven.api.JoinAddition;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class CountDistinct implements Aggregation {

    public static CountDistinct of(JoinAddition addition) {
        return ImmutableCountDistinct.builder().addition(addition).build();
    }

    public static CountDistinct of(String x) {
        return of(JoinAddition.parse(x));
    }

    public abstract JoinAddition addition();

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
