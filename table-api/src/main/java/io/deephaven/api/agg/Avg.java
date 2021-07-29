package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class Avg implements Aggregation {

    public static Avg of(Pair pair) {
        return ImmutableAvg.of(pair);
    }

    public static Avg of(String x) {
        return of(Pair.parse(x));
    }

    @Parameter
    public abstract Pair pair();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
