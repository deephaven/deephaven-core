//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.ComparableRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.compare.ObjectComparisons;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Applies a {@link ComparableRangeFilter} or an object-typed {@link MatchFilter} to one row group's {@code min}/{@code
 * max} statistics, answering whether that row group could hold a matching row.
 * <p>
 * This is the fallback for column types with no handler of their own -- dates and times among them -- and serves only
 * those {@link MinMaxFromStatistics#getMinMaxForComparable} can decode; for anything else it declines. Values are
 * ordered with {@link io.deephaven.util.compare.ObjectComparisons}, matching the engine. {@link String} is deliberately
 * <i>not</i> served here, because Parquet orders those statistics by unsigned bytes rather than by
 * {@link String#compareTo}; see {@link StringPushdownHandler}.
 *
 * <h2>Nulls</h2>
 *
 * Of the two sources of a Deephaven null that {@link StatisticsEvaluator} describes, neither is this class's business:
 * these types have no sentinel encoding, so a Deephaven null comes solely from a Parquet null, and a null is simply
 * dropped from the filter's values here.
 * <p>
 * <b>These methods are not correct in isolation</b> for a filter that a null row satisfies; reach them through
 * {@code StatisticsEvaluator.maybeMakeForFilter}, which gates on those rows.
 * <p>
 * A value that is not {@link Comparable} is a different matter and is declined outright, having no place in the
 * ordering at all.
 */
final class ComparablePushdownHandler {

    /**
     * Resolves {@code filter} to an evaluator, or returns {@code null} if this handler does not serve it. This is the
     * entry point {@link StatisticsEvaluator} dispatches through, and is tried last: it claims whatever earlier
     * handlers did not, for the column types {@link MinMaxFromStatistics#canDecodeComparable can be decoded}. A type
     * that cannot be decoded is declined here rather than per row group, so the caller can skip the row groups
     * outright.
     */
    @Nullable
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final WhereFilter filter) {
        if (filter instanceof ComparableRangeFilter) {
            final ComparableRangeFilter rangeFilter = (ComparableRangeFilter) filter;
            return MinMaxFromStatistics.canDecodeComparable(rangeFilter.getColumnType())
                    ? maybeCreateEvaluator(rangeFilter)
                    : null;
        }
        if (filter instanceof MatchFilter) {
            final MatchFilter matchFilter = (MatchFilter) filter;
            return MinMaxFromStatistics.canDecodeComparable(matchFilter.getColumnType())
                    ? maybeCreateEvaluator(matchFilter)
                    : null;
        }
        return null;
    }

    /**
     * Prepares the range filter for evaluation: whether the statistics range intersects the range it defines. A null
     * bound resolves to {@link StatisticsEvaluator#ALWAYS_MAYBE} here, so the caller can skip the row groups rather
     * than ask about each in turn.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final ComparableRangeFilter comparableRangeFilter) {
        final Comparable<?> dhLower = comparableRangeFilter.getLower();
        final Comparable<?> dhUpper = comparableRangeFilter.getUpper();
        if (dhLower == null || dhUpper == null) {
            // Not reachable from a parsed comparison: RangeFilter always supplies both bounds. Nor is it clear what
            // a null bound should mean -- Deephaven orders null below every value, so a null lower reads as
            // "unbounded below" while a null upper reads as an empty range, and no filter expresses either. Declined
            // rather than guessed.
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        // Get the column type from the filter
        final Class<?> dhColumnType = comparableRangeFilter.getColumnType();
        if (dhColumnType == null) {
            throw new IllegalStateException("Filter not initialized with a column type: " + comparableRangeFilter);
        }
        final boolean lowerInclusive = comparableRangeFilter.isLowerInclusive();
        final boolean upperInclusive = comparableRangeFilter.isUpperInclusive();
        return statistics -> {
            final MutableObject<Comparable<?>> mutableMin = new MutableObject<>();
            final MutableObject<Comparable<?>> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForComparable(statistics, mutableMin::setValue, mutableMax::setValue,
                    dhColumnType)) {
                // Statistics could not be processed, so we assume that we overlap.
                return true;
            }
            return maybeOverlapsRangeImpl(
                    mutableMin.get(), mutableMax.get(),
                    dhLower, lowerInclusive,
                    dhUpper, upperInclusive);
        };
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsRangeImpl(
            @NotNull final Comparable<?> min, @NotNull final Comparable<?> max,
            @NotNull final Comparable<?> lower, final boolean lowerInclusive,
            @NotNull final Comparable<?> upper, final boolean upperInclusive) {
        if ((upperInclusive && lowerInclusive)
                ? ObjectComparisons.gt(lower, upper)
                : ObjectComparisons.geq(lower, upper)) {
            return false; // Empty range, no overlap
        }
        // Following logic assumes (min, max) to be a continuous range and not granular. So (a,b) will be considered
        // as "maybe overlapping" with [a, b] where b follows immediately after a.
        return (upperInclusive ? ObjectComparisons.leq(min, upper) : ObjectComparisons.lt(min, upper))
                && (lowerInclusive ? ObjectComparisons.geq(max, lower) : ObjectComparisons.gt(max, lower));
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in the filter.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getMatchOptions().inverted();
        if (values == null || values.length == 0) {
            // No values to check against
            return invertMatch ? StatisticsEvaluator.ALWAYS_MAYBE : statistics -> false;
        }
        // Nulls are dropped here; the null gate in StatisticsEvaluator answers for them.
        final Comparable<?>[] allValues = new Comparable[values.length];
        int numNonNull = 0;
        for (final Object value : values) {
            if (value instanceof Comparable) {
                allValues[numNonNull++] = (Comparable<?>) value;
            } else if (value != null) {
                // Not Comparable, so it cannot be ordered against the statistics at all.
                return StatisticsEvaluator.ALWAYS_MAYBE;
            }
            // A null falls through and is simply dropped.
        }
        if (numNonNull == 0) {
            // Nothing but null was given. `X != null` matches any non-null value, and usable statistics guarantee
            // at least one exists; `X == null` reaches here only for a row group with no Parquet nulls to match.
            return invertMatch
                    ? StatisticsEvaluator.ALWAYS_MAYBE
                    : statistics -> false;
        }
        final Comparable<?>[] comparableValues =
                numNonNull == values.length ? allValues : Arrays.copyOf(allValues, numNonNull);
        // Get the column type from the filter
        final Class<?> dhColumnType = matchFilter.getColumnType();
        if (dhColumnType == null) {
            throw new IllegalStateException("Filter not initialized with a column type: " + matchFilter);
        }
        if (invertMatch) {
            // Sorted once here; maybeMatchesInverse walks the gaps between adjacent values. Natural
            // ordering matches the ObjectComparisons the gap walk uses, for the non-null values that
            // are all that reach this point -- nulls were removed above.
            Arrays.sort(comparableValues);
        }
        return statistics -> {
            final MutableObject<Comparable<?>> mutableMin = new MutableObject<>();
            final MutableObject<Comparable<?>> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForComparable(statistics, mutableMin::setValue, mutableMax::setValue,
                    dhColumnType)) {
                // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
                return true;
            }
            return invertMatch
                    ? maybeMatchesInverse(mutableMin.get(), mutableMax.get(), comparableValues)
                    : maybeMatches(mutableMin.get(), mutableMax.get(), comparableValues);
        };
    }


    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    static boolean maybeMatches(
            @NotNull final Comparable<?> min,
            @NotNull final Comparable<?> max,
            @NotNull final Comparable<?>[] values) {
        for (final Comparable<?> value : values) {
            if (maybeOverlapsRangeImpl(min, max, value, true, value, true)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Verifies that the {@code [min, max]} range includes any value that is not in the given {@code values} array. This
     * is done by checking whether {@code [min, max]} overlaps with every open gap produced by excluding the given
     * values. For example, if the values are sorted as {@code v_0, v_1, ..., v_n-1}, then the gaps are:
     *
     * <pre>
     * [..., v_0), (v_0, v_1), . . , (v_n-2, v_n-1), (v_n-1, ...]
     * </pre>
     * 
     * where {@code ...} represents the extreme ends of the range.
     */
    static boolean maybeMatchesInverse(
            @NotNull final Comparable<?> min,
            @NotNull final Comparable<?> max,
            @NotNull final Comparable<?>[] values) {
        if (ObjectComparisons.lt(min, values[0])) {
            return true;
        }
        final int numValues = values.length;
        for (int i = 0; i < numValues - 1; i++) {
            if (maybeOverlapsRangeImpl(min, max, values[i], false, values[i + 1], false)) {
                return true;
            }
        }
        return ObjectComparisons.gt(max, values[values.length - 1]);
    }
}
