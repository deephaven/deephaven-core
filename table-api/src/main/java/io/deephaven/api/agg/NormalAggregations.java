package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.agg.spec.AggSpec;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * Normal aggregations is an {@link Aggregation} that is composed of a {@link #spec() spec} and multiple {@link #pairs()
 * pairs}.
 */
@Immutable
@BuildableStyle
public abstract class NormalAggregations implements Aggregation {

    public static Builder builder() {
        return ImmutableNormalAggregations.builder();
    }

    public abstract AggSpec spec();

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
                    String.format("%s should have at least two pairs", NormalAggregations.class));
        }
    }

    public interface Builder {
        Builder spec(AggSpec spec);

        Builder addPairs(Pair element);

        Builder addPairs(Pair... elements);

        Builder addAllPairs(Iterable<? extends Pair> elements);

        NormalAggregations build();
    }
}
