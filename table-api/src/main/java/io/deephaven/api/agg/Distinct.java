package io.deephaven.api.agg;

import io.deephaven.api.JoinAddition;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Distinct implements Aggregation {

    public static Distinct of(JoinAddition addition) {
        return ImmutableDistinct.builder().addition(addition).build();
    }

    public static Distinct of(String x) {
        return of(JoinAddition.parse(x));
    }

    public abstract JoinAddition addition();

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
