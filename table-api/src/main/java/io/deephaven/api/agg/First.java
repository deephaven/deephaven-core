package io.deephaven.api.agg;

import io.deephaven.api.JoinMatch;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class First implements Aggregation {

    public static First of(JoinMatch match) {
        return ImmutableFirst.of(match);
    }

    public static First of(String x) {
        return of(JoinMatch.parse(x));
    }

    @Parameter
    public abstract JoinMatch match();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
