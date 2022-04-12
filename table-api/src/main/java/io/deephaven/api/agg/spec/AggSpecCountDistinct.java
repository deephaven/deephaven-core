package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the count of distinct values for each group.
 */
@Immutable
@BuildableStyle
public abstract class AggSpecCountDistinct extends AggSpecBase {

    public static AggSpecCountDistinct of() {
        return ImmutableAggSpecCountDistinct.builder().build();
    }

    public static AggSpecCountDistinct of(boolean countNulls) {
        return ImmutableAggSpecCountDistinct.builder().countNulls(countNulls).build();
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
    @Default
    public boolean countNulls() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
