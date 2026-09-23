//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.SingleSidedComparableRangeFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.compare.ObjectComparisons;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

final class SingleSidedComparableRangePushdownHandler {

    /**
     * Resolves {@code filter} to an evaluator, or returns {@code null} if this handler does not serve it. A column type
     * {@link MinMaxFromStatistics#canDecodeComparable cannot decode} is declined here rather than per row group.
     */
    @Nullable
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final WhereFilter filter) {
        if (!(filter instanceof final SingleSidedComparableRangeFilter rangeFilter)) {
            return null;
        }
        return MinMaxFromStatistics.canDecodeComparable(rangeFilter.getColumnType())
                ? maybeCreateEvaluator(rangeFilter)
                : null;
    }

    /**
     * Prepares the single-sided range filter for evaluation. A null pivot resolves to
     * {@link StatisticsEvaluator#ALWAYS_MAYBE} here, so the caller can skip the row groups rather than ask about each
     * in turn.
     */
    static StatisticsEvaluator maybeCreateEvaluator(final SingleSidedComparableRangeFilter sscrf) {
        final Comparable<?> pivot = sscrf.getPivot();
        final boolean isGreaterThan = sscrf.isGreaterThan();
        if (sscrf.isLowerInclusive() != sscrf.isUpperInclusive()) {
            throw new IllegalStateException("SingleSidedComparableRangeFilter must have both bounds inclusive or " +
                    "exclusive: " + sscrf);
        }
        final boolean isInclusive = sscrf.isLowerInclusive();
        if (pivot == null) {
            // Not reachable from a parsed comparison, and null is not orderable against itself here: Deephaven
            // orders null below every value, so `X < null` is empty and `X > null` is everything, and no parsed
            // filter expresses either. Declined rather than guessed.
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        // `X < v` and `X <= v` accept a null row, since Deephaven orders null under every value; `X > v` cannot.
        // These types have no sentinel encoding, so the null-aware check in NullAwareEvaluator settles that on its own.
        final Class<?> dhColumnType = sscrf.getColumnType();
        if (dhColumnType == null) {
            throw new IllegalStateException("Filter not initialized with a column type: " + sscrf);
        }
        if (isGreaterThan) {
            return statistics -> {
                final Comparable<?>[] minMax = decodeMinMax(statistics, dhColumnType);
                // Some value can exceed the pivot only if the largest one does.
                return minMax == null || (isInclusive
                        ? ObjectComparisons.geq(minMax[1], pivot)
                        : ObjectComparisons.gt(minMax[1], pivot));
            };
        }
        return statistics -> {
            final Comparable<?>[] minMax = decodeMinMax(statistics, dhColumnType);
            // ... and fall below it only if the smallest one does.
            return minMax == null || (isInclusive
                    ? ObjectComparisons.leq(minMax[0], pivot)
                    : ObjectComparisons.lt(minMax[0], pivot));
        };
    }

    /**
     * Reads this row group's extremes as {@code {min, max}}, or returns {@code null} if the statistics cannot be used.
     */
    @Nullable
    private static Comparable<?>[] decodeMinMax(
            @NotNull final Statistics<?> statistics,
            @NotNull final Class<?> dhColumnType) {
        final Comparable<?>[] minMax = new Comparable<?>[2];
        if (!MinMaxFromStatistics.getMinMaxForComparable(statistics,
                v -> minMax[0] = v, v -> minMax[1] = v, dhColumnType)) {
            // Statistics could not be processed.
            return null;
        }
        return minMax;
    }
}
