//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.SingleSidedComparableRangeFilter;
import io.deephaven.util.compare.ObjectComparisons;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;

final class SingleSidedComparableRangePushdownHandler {

    static boolean maybeOverlaps(
            final SingleSidedComparableRangeFilter sscrf,
            final Statistics<?> statistics) {
        final Comparable<?> pivot = sscrf.getPivot();
        final boolean isGreaterThan = sscrf.isGreaterThan();
        if (sscrf.isLowerInclusive() != sscrf.isUpperInclusive()) {
            throw new IllegalStateException("SingleSidedComparableRangeFilter must have both bounds inclusive or " +
                    "exclusive: " + sscrf);
        }
        final boolean isInclusive = sscrf.isLowerInclusive();
        if (pivot == null) {
            // A null pivot is not produced by any parsed comparison, and null is not orderable against itself here.
            // TODO (DH-19666): Improve handling of nulls
            return true;
        }
        // Note `X < v` and `X <= v` are evaluated too. They match null rows -- Deephaven orders null below every
        // value -- which is why this used to decline them, but that is now accounted for once by the null guard in
        // ParquetTableLocation.pushdownRowGroupMetadata.
        final Class<?> dhColumnType = sscrf.getColumnType();
        if (dhColumnType == null) {
            throw new IllegalStateException("Filter not initialized with a column type: " + sscrf);
        }
        final MutableObject<Comparable<?>> mutableMin = new MutableObject<>();
        final MutableObject<Comparable<?>> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForComparable(statistics, mutableMin::setValue, mutableMax::setValue,
                dhColumnType)) {
            // Statistics could not be processed, so we assume that we overlap.
            return true;
        }
        return maybeOverlapsImpl(
                mutableMin.getValue(), mutableMax.getValue(),
                pivot, isInclusive, isGreaterThan);
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given pivot.
     */
    private static boolean maybeOverlapsImpl(
            final Comparable<?> min, final Comparable<?> max,
            final Comparable<?> pivot, final boolean inclusive, final boolean isGreaterThan) {
        return isGreaterThan ? (inclusive ? ObjectComparisons.geq(max, pivot) : ObjectComparisons.gt(max, pivot))
                : (inclusive ? ObjectComparisons.leq(min, pivot) : ObjectComparisons.lt(min, pivot));
    }
}
