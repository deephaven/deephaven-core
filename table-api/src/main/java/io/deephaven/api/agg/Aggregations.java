package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.List;

/**
 * Aggregations is an {@link Aggregation} that is a collection of two or more {@link Aggregation aggregations}.
 */
@Immutable
@BuildableStyle
public abstract class Aggregations implements Aggregation {

    public static Aggregations.Builder builder() {
        return ImmutableAggregations.builder();
    }

    public abstract List<Aggregation> aggregations();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkSize() {
        if (aggregations().size() < 2) {
            throw new IllegalArgumentException(
                    String.format("%s should have at least two aggregations", Aggregations.class));
        }
    }

    public interface Builder {
        Builder addAggregations(Aggregation aggregation);

        Builder addAggregations(Aggregation... aggregations);

        Builder addAllAggregations(Iterable<? extends Aggregation> aggregations);

        Aggregations build();
    }
}
