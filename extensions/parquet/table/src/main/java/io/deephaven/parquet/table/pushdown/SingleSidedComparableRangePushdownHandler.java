//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ObjectChunkFilter;
import io.deephaven.engine.table.impl.select.SingleSidedComparableRangeFilter;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.ObjectComparisons;

@InternalUseOnly
public abstract class SingleSidedComparableRangePushdownHandler {

    public static boolean maybeOverlaps(
            final SingleSidedComparableRangeFilter sscrf,
            final ChunkFilter chunkFilter,
            final MinMax<?> minMax,
            final long nullCount) {
        final Comparable<?> min = minMax.min();
        final Comparable<?> max = minMax.max();
        if (!(chunkFilter instanceof ObjectChunkFilter)) {
            // If the chunk filter is not an ObjectChunkFilter, we cannot handle it.
            return true;
        }
        final ObjectChunkFilter<Comparable<?>> objectChunkFilter = (ObjectChunkFilter<Comparable<?>>) chunkFilter;
        final boolean matchesNull = nullCount > 0 && objectChunkFilter.matches(null);
        if (matchesNull) {
            return true;
        }
        return maybeOverlaps(min, max,
                sscrf.getPivot(), sscrf.isGreaterThan(),
                sscrf.isLowerInclusive(), sscrf.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given pivot.
     */
    private static boolean maybeOverlaps(
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
