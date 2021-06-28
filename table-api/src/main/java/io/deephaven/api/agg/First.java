package io.deephaven.api.agg;

import io.deephaven.api.JoinAddition;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class First implements Aggregation {

    public static First of(JoinAddition addition) {
        return ImmutableFirst.of(addition);
    }

    public static First of(String x) {
        return of(JoinAddition.parse(x));
    }

    @Parameter
    public abstract JoinAddition addition();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
