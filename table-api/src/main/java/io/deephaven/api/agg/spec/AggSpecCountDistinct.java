/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Specifies an aggregation that outputs the count of distinct values for each group.
 */
@Immutable
@SimpleStyle
public abstract class AggSpecCountDistinct extends AggSpecBase {

    public static final boolean COUNT_NULLS_DEFAULT = false;

    /**
     * Create a new AggSpecCountDistinct with {@code countNulls} of {@value COUNT_NULLS_DEFAULT}.
     *
     * @return the agg spec
     */
    public static AggSpecCountDistinct of() {
        return of(COUNT_NULLS_DEFAULT);
    }

    /**
     * Create a new AggSpecCountDistinct.
     *
     * @param countNulls if nulls should be counted
     * @return the agg spec
     */
    public static AggSpecCountDistinct of(boolean countNulls) {
        return ImmutableAggSpecCountDistinct.of(countNulls);
    }

    @Override
    public final String description() {
        return "count distinct" + (countNulls() ? " (counting nulls)" : "");
    }

    /**
     * Whether {@code null} input values should be included when counting the distinct input values.
     * 
     * @return Whether to count nulls
     */
    @Parameter
    public abstract boolean countNulls();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
