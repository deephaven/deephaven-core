/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Specifies an aggregation that outputs the distinct values for each group as a Deephaven vector
 * (io.deephaven.vector.Vector).
 */
@Immutable
@SimpleStyle
public abstract class AggSpecDistinct extends AggSpecBase {

    public static final boolean INCLUDE_NULLS_DEFAULT = false;

    /**
     * Create a new AggSpecDistinct with {@code includeNulls} of {@value INCLUDE_NULLS_DEFAULT}.
     *
     * @return the agg spec
     */
    public static AggSpecDistinct of() {
        return of(INCLUDE_NULLS_DEFAULT);
    }

    /**
     * Create a new AggSpecDistinct.
     *
     * @param includeNulls if nulls should be included as distinct values
     * @return the agg spec
     */
    public static AggSpecDistinct of(boolean includeNulls) {
        return ImmutableAggSpecDistinct.of(includeNulls);
    }

    @Override
    public final String description() {
        return "distinct" + (includeNulls() ? " (including nulls)" : "");
    }

    /**
     * Whether {@code null} input values should be included in the distinct output values.
     *
     * @return Whether to include nulls
     */
    @Parameter
    public abstract boolean includeNulls();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
