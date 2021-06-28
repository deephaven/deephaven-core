package io.deephaven.api.agg;

import io.deephaven.api.JoinAddition;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Unique implements Aggregation {

    public static Unique of(JoinAddition addition) {
        return ImmutableUnique.builder().addition(addition).build();
    }

    public static Unique of(String x) {
        return of(JoinAddition.parse(x));
    }

    public abstract JoinAddition addition();

    @Value.Default
    public boolean includeNulls() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
