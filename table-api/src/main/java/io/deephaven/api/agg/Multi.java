package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * An aggregation that is a list of other aggregations. Useful as a helper when returning an
 * aggregation constructed via a varargs parameter.
 *
 * @param <AGG> the aggregation type
 * @see AggregationFinisher#of(String...)
 */
@Immutable
@BuildableStyle
public abstract class Multi<AGG extends Aggregation> implements Aggregation {

    public static <AGG extends Aggregation> Builder<AGG> builder() {
        return ImmutableMulti.builder();
    }

    public abstract List<AGG> aggregations();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkSize() {
        if (aggregations().size() < 2) {
            throw new IllegalArgumentException(
                String.format("%s should have at least two aggregations", Multi.class));
        }
    }

    public interface Builder<AGG extends Aggregation> {
        Builder<AGG> addAggregations(AGG element);

        Builder<AGG> addAggregations(AGG... elements);

        Builder<AGG> addAllAggregations(Iterable<? extends AGG> elements);

        Multi<AGG> build();
    }
}
