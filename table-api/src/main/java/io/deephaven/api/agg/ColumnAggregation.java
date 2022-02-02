package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.agg.spec.AggSpec;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * A ColumnAggregation is an {@link Aggregation} that is composed of a {@link #spec() spec} and a single input/output
 * column {@link #pair() pair}. The spec defines the aggregation operation to apply to the input column in order to
 * produce the paired output column.
 */
@Immutable
@SimpleStyle
public abstract class ColumnAggregation implements Aggregation {

    public static ColumnAggregation of(AggSpec spec, Pair pair) {
        return ImmutableColumnAggregation.of(spec, pair);
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
