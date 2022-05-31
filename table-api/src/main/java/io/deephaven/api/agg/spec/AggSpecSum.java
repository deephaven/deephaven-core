package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the sum of input values for each group. Only works with numeric input types and
 * {@link Boolean}.
 * <p>
 * {@link Boolean} inputs are aggregated according to the following rules:
 * <ul>
 * <li>If any input value is {@code true}, the output value is {@code true}</li>
 * <li>If there are no non-{@code null} input values, the output value is {@code null}</li>
 * <li>Else all input values must be {@code false}, and the output value is {@code false}</li>
 * </ul>
 *
 * @see TableOperations#sumBy
 */
@Immutable
@SimpleStyle
public abstract class AggSpecSum extends AggSpecEmptyBase {

    public static AggSpecSum of() {
        return ImmutableAggSpecSum.of();
    }

    @Override
    public final String description() {
        return "sum";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
