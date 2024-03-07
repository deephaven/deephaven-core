//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Specifier for a column aggregation that produces a median value from the input column's values for each group. Only
 * works for numeric or {@link Comparable} input types.
 */
@Immutable
@SimpleStyle
public abstract class AggSpecMedian extends AggSpecBase {

    public static final boolean AVERAGE_EVENLY_DIVIDED_DEFAULT = true;

    /**
     * Create a new AggSpecMedian with {@code averageEvenlyDivided} of {@value AVERAGE_EVENLY_DIVIDED_DEFAULT}.
     *
     * @return the agg spec
     */
    public static AggSpecMedian of() {
        return of(AVERAGE_EVENLY_DIVIDED_DEFAULT);
    }

    /**
     * Create a new AggSpecMedian.
     *
     * @param averageEvenlyDivided see {@link #averageEvenlyDivided()}
     * @return the agg spec
     */
    public static AggSpecMedian of(boolean averageEvenlyDivided) {
        return ImmutableAggSpecMedian.of(averageEvenlyDivided);
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
    @Parameter
    public abstract boolean averageEvenlyDivided();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
