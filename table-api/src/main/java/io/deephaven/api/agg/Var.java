package io.deephaven.api.agg;

import io.deephaven.api.JoinAddition;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class Var implements Aggregation {

    public static Var of(JoinAddition addition) {
        return ImmutableVar.of(addition);
    }

    public static Var of(String x) {
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
