package io.deephaven.api.agg;

import io.deephaven.api.JoinAddition;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class Array implements Aggregation {

    public static Array of(JoinAddition addition) {
        return ImmutableArray.of(addition);
    }

    public static Array of(String x) {
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
