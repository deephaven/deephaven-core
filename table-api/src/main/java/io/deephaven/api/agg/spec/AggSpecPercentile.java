package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

/**
 * Specifier for a column aggregation that produces a percentile value from the input column's values for each group.
 * Only works for numeric or {@link Comparable} input types.
 */
@Immutable
@BuildableStyle
public abstract class AggSpecPercentile extends AggSpecBase {

    public static AggSpecPercentile of(double percentile) {
        return ImmutableAggSpecPercentile.builder().percentile(percentile).build();
    }

    public static AggSpecPercentile of(double percentile, boolean averageEvenlyDivided) {
        return ImmutableAggSpecPercentile.builder().percentile(percentile).averageEvenlyDivided(averageEvenlyDivided)
                .build();
    }

    @Override
    public final String description() {
        return String.format("%.2f percentile%s",
                percentile(),
                averageEvenlyDivided() ? " (averaging when evenly divided)" : "");
    }

    /**
     * The percentile to calculate. Must be &gt;= 0.0 and &lt;= 1.0.
     *
     * @return The percentile to calculate
     */
    public abstract double percentile();

    /**
     * Whether to average the highest low-bucket value and lowest high-bucket value, when the low-bucket and high-bucket
     * are of equal size. Only applies to numeric types.
     *
     * @return Whether to average the two result candidates for evenly-divided input sets of numeric types
     */
    @Default
    public boolean averageEvenlyDivided() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkPercentile() {
        if (percentile() < 0.0 || percentile() > 1.0) {
            throw new IllegalArgumentException("Percentile must be in range [0.0, 1.0]");
        }
    }
}
