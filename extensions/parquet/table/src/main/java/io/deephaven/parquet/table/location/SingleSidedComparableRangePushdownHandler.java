//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.SingleSidedComparableRangeFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.compare.ObjectComparisons;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

final class SingleSidedComparableRangePushdownHandler {

    /**
     * Resolves {@code filter} to an evaluator, or returns {@code null} if this handler does not serve it. A column type
     * {@link MinMaxFromStatistics#canDecodeComparable cannot decode} is declined here rather than per row group.
     */
    @Nullable
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final WhereFilter filter) {
        if (!(filter instanceof SingleSidedComparableRangeFilter)) {
            return null;
        }
        final SingleSidedComparableRangeFilter rangeFilter = (SingleSidedComparableRangeFilter) filter;
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
        // These types have no sentinel encoding, so the null gate in maybeMakeForFilter settles that on its own.
        final Class<?> dhColumnType = sscrf.getColumnType();
        if (dhColumnType == null) {
            throw new IllegalStateException("Filter not initialized with a column type: " + sscrf);
        }
        return statistics -> {
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
        };
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
