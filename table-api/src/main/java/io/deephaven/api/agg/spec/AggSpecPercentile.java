//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Specifier for a column aggregation that produces a percentile value from the input column's values for each group.
 * Only works for numeric or {@link Comparable} input types.
 */
@Immutable
@SimpleStyle
public abstract class AggSpecPercentile extends AggSpecBase {

    public static final boolean AVERAGE_EVENLY_DIVIDED = false;

    /**
     * Create a new AggSpecPercentile with {@code averageEvenlyDivided} of {@value AVERAGE_EVENLY_DIVIDED}.
     *
     * @param percentile the percentile
     * @return the agg spec
     */
    public static AggSpecPercentile of(double percentile) {
        return of(percentile, AVERAGE_EVENLY_DIVIDED);
    }

    /**
     * Create a new AggSpecPercentile.
     *
     * @param percentile the percentile
     * @param averageEvenlyDivided see {@link #averageEvenlyDivided()}
     * @return the agg spec
     */
    public static AggSpecPercentile of(double percentile, boolean averageEvenlyDivided) {
        return ImmutableAggSpecPercentile.of(percentile, averageEvenlyDivided);
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
    @Parameter
    public abstract double percentile();

    /**
     * Whether to average the highest low-bucket value and lowest high-bucket value, when the low-bucket and high-bucket
     * are of equal size. Only applies to numeric types.
     *
     * @return Whether to average the two result candidates for evenly-divided input sets of numeric types
     */
    @Parameter
    public abstract boolean averageEvenlyDivided();

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
