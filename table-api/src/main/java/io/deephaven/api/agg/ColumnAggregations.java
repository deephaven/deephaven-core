package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.agg.spec.AggSpec;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * ColumnAggregations is an {@link Aggregation} that is composed of a {@link #spec() spec} and multiple input/output
 * column {@link #pairs() pairs}. The spec defines the aggregation operation to apply to each input column in order to
 * produce the paired output column.
 */
@Immutable
@BuildableStyle
public abstract class ColumnAggregations implements Aggregation {

    public static Builder builder() {
        return ImmutableColumnAggregations.builder();
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
                    String.format("%s should have at least two pairs", ColumnAggregations.class));
        }
    }

    public interface Builder {
        Builder spec(AggSpec spec);

        Builder addPairs(Pair element);

        Builder addPairs(Pair... elements);

        Builder addAllPairs(Iterable<? extends Pair> elements);

        ColumnAggregations build();
    }
}
