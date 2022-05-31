package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the distinct values for each group as a Deephaven vector
 * (io.deephaven.vector.Vector).
 */
@Immutable
@BuildableStyle
public abstract class AggSpecDistinct extends AggSpecBase {

    public static AggSpecDistinct of() {
        return ImmutableAggSpecDistinct.builder().build();
    }

    public static AggSpecDistinct of(boolean includeNulls) {
        return ImmutableAggSpecDistinct.builder().includeNulls(includeNulls).build();
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
    @Default
    public boolean includeNulls() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
