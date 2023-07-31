/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SingletonStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the sum of absolute input values for each group. Only works with numeric input
 * types and {@link Boolean}.
 * <p>
 * {@link Boolean} inputs are aggregated according to the following rules:
 * <ul>
 * <li>If any input value is {@code true}, the output value is {@code true}</li>
 * <li>If there are no non-{@code null} input values, the output value is {@code null}</li>
 * <li>Else all input values must be {@code false}, and the output value is {@code false}</li>
 * </ul>
 *
 * @see TableOperations#absSumBy
 */
@Immutable
@SingletonStyle
public abstract class AggSpecAbsSum extends AggSpecEmptyBase {

    public static AggSpecAbsSum of() {
        return ImmutableAggSpecAbsSum.of();
    }

    @Override
    public final String description() {
        return "absolute sum";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
