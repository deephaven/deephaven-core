package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.agg.spec.AggSpec;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * A normal aggregation is an {@link Aggregation} that is composed of a {@link #spec() spec} and a {@link #pair() pair}.
 */
@Immutable
@SimpleStyle
public abstract class NormalAggregation implements Aggregation {

    public static NormalAggregation of(AggSpec spec, Pair pair) {
        return ImmutableNormalAggregation.of(spec, pair);
    }

    @Parameter
    public abstract AggSpec spec();

    @Parameter
    public abstract Pair pair();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
