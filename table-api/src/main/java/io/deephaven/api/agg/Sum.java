package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class Sum implements Aggregation {

    public static Sum of(Pair pair) {
        return ImmutableSum.of(pair);
    }

    public static Sum of(String x) {
        return of(Pair.parse(x));
    }

    @Parameter
    public abstract Pair pair();

    public final AbsSum abs() {
        return AbsSum.of(pair());
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
