package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.agg.key.Key;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * Keyed aggregations is an {@link Aggregation} that is composed of a {@link #key() key} and multiple {@link #pairs()
 * pairs}.
 */
@Immutable
@BuildableStyle
public abstract class KeyedAggregations implements Aggregation {

    public static Builder builder() {
        return ImmutableKeyedAggregations.builder();
    }

    public abstract Key key();

    public abstract List<Pair> pairs();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkSize() {
        if (pairs().size() < 2) {
            throw new IllegalArgumentException(
                    String.format("%s should have at least two pairs", KeyedAggregations.class));
        }
    }

    public interface Builder {
        Builder key(Key key);

        Builder addPairs(Pair element);

        Builder addPairs(Pair... elements);

        Builder addAllPairs(Iterable<? extends Pair> elements);

        KeyedAggregations build();
    }
}
