//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.agg.util.AggCountType;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Specifies an aggregation that outputs the count of values for each group.
 */
@Immutable
@SimpleStyle
public abstract class AggSpecCountValues extends AggSpecBase {

    /**
     * Create a new AggSpecCountValues with {@code countType} of {@code NON_NULL}.
     *
     * @return the agg spec
     */
    public static AggSpecCountValues of() {
        return of(AggCountType.NON_NULL);
    }

    /**
     * Create a new AggSpecCountValues with the specified {@code countType}.
     *
     * @param countType the count type for this aggregation
     * @return the agg spec
     */
    public static AggSpecCountValues of(final AggCountType countType) {
        return ImmutableAggSpecCountValues.of(countType);
    }

    @Override
    public final String description() {
        return "count type" + countType();
    }

    /**
     * Whether {@code null} input values should be included when counting the distinct input values.
     * 
     * @return Whether to count nulls
     */
    @Parameter
    public abstract AggCountType countType();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
