//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.engine.table.impl.select.SingleSidedComparableRangeFilter;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.ObjectComparisons;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;

@InternalUseOnly
public abstract class SingleSidedComparableRangePushdownHandler {

    public static boolean maybeOverlaps(
            final SingleSidedComparableRangeFilter sscrf,
            final Statistics<?> statistics) {
        final Comparable<?> pivot = sscrf.getPivot();
        final boolean isGreaterThan = sscrf.isGreaterThan();
        if (pivot == null || !isGreaterThan) {
            // Skip pushdown-based filtering for nulls, which are considered smaller than any value.
            return true;
        }
        final Class<?> dhColumnType = sscrf.getColumnType();
        final MutableObject<Comparable<?>> mutableMin = new MutableObject<>();
        final MutableObject<Comparable<?>> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForComparable(statistics, mutableMin::setValue, mutableMax::setValue,
                dhColumnType)) {
            // Statistics could not be processed, so we assume that we overlap.
            return true;
        }
        return maybeOverlapsImpl(
                mutableMin.getValue(), mutableMax.getValue(),
                pivot, sscrf.isGreaterThan(),
                sscrf.isLowerInclusive(), sscrf.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given pivot.
     */
    private static boolean maybeOverlapsImpl(
            final Comparable<?> min, final Comparable<?> max,
            final Comparable<?> pivot, final boolean isGreaterThan,
            final boolean lowerInclusive, final boolean upperInclusive) {
        if (isGreaterThan) {
            final int c1 = ObjectComparisons.compare(pivot, max);
            if (c1 > 0) {
                // pivot > max, no overlap possible.
                return false;
            }
            if (c1 == 0 && !upperInclusive) {
                // pivot == max, but upper is not inclusive, no overlap possible.
                return false;
            }
        } else {
            final int c2 = ObjectComparisons.compare(min, pivot);
            if (c2 > 0) {
                // min > pivot, no overlap possible.
                return false;
            }
            if (c2 == 0 && !lowerInclusive) {
                // min == pivot, but lower is not inclusive, no overlap possible.
                return false;
            }
        }
        return true;
    }
}
