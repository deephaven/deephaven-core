package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.agg.key.Key;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * A keyed aggregation is an {@link Aggregation} that is composed of a {@link #key() key} and a {@link #pair() pair}.
 */
@Immutable
@SimpleStyle
public abstract class KeyedAggregation implements Aggregation {

    public static KeyedAggregation of(Key key, Pair pair) {
        return ImmutableKeyedAggregation.of(key, pair);
    }

    @Parameter
    public abstract Key key();

    @Parameter
    public abstract Pair pair();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
