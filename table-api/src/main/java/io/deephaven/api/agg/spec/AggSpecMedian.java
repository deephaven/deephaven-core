package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

/**
 * Specifier for a column aggregation that produces a median value from the input column's values for each group. Only
 * works for numeric or {@link Comparable} input types.
 */
@Immutable
@BuildableStyle
public abstract class AggSpecMedian extends AggSpecBase {

    public static AggSpecMedian of() {
        return ImmutableAggSpecMedian.builder().build();
    }

    public static AggSpecMedian of(boolean averageEvenlyDivided) {
        return ImmutableAggSpecMedian.builder().averageEvenlyDivided(averageEvenlyDivided).build();
    }

    @Override
    public final String description() {
        return "median" + (averageEvenlyDivided() ? " (averaging when evenly divided)" : "");
    }

    /**
     * Whether to average the highest low-bucket value and lowest high-bucket value, when the low-bucket and high-bucket
     * are of equal size. Only applies to numeric types.
     *
     * @return Whether to average the two result candidates for evenly-divided input sets of numeric types
     */
    @Default
    public boolean averageEvenlyDivided() {
        return true;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
