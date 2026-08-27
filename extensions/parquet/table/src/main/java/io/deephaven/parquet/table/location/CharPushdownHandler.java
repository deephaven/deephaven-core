//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.CharRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Applies a {@link CharRangeFilter} or a char-typed {@link MatchFilter} to one row group's {@code min}/{@code max}
 * statistics, answering whether that row group could hold a matching row.
 *
 * <h2>Usage</h2>
 *
 * For a match filter, call {@link #maybeCreateEvaluator} once and apply the evaluator it returns to each row group in
 * turn. Unboxing the filter's values and sorting them for the inverted walk both happen during that single call, rather
 * than once per row group. {@link #maybeOverlaps(MatchFilter, Statistics)} performs both steps for a single row group.
 * {@link #maybeOverlaps(CharRangeFilter, Statistics)} serves range filters, which need no preparation.
 * <p>
 * The interval arithmetic lives in {@link #maybeOverlapsRangeImpl}. {@link #maybeMatches} reuses it by testing each of
 * the filter's values as the closed range {@code [v, v]}, and {@link #maybeMatchesInverse} by testing the gaps between
 * adjacent values.
 *
 * <h2>Nulls</h2>
 *
 * {@link StatisticsEvaluator} describes the two reasons a row can read back as null in Deephaven. This class answers
 * for one of them and not the other.
 * <ul>
 * <li>A <b>stored sentinel</b> -- a value equal to {@code NULL_CHAR} -- is this class's business. To Parquet it is an
 * ordinary value, sitting inside {@code min}/{@code max} like any other, so the tests here account for it: a match
 * filter keeps the sentinel among its values, and an unbounded-below range filter looks for it explicitly.</li>
 * <li>A <b>Parquet null</b> is not. Such a row is invisible to {@code min}/{@code max}, so nothing here can see one or
 * rule one out. {@code StatisticsEvaluator.maybeMakeForFilter} gates on that before any of this runs.</li>
 * </ul>
 * These methods therefore answer from {@code min}/{@code max} alone, and are <b>not</b> correct in isolation for a
 * filter that a null row satisfies -- {@code X == null}, {@code X != v}, {@code X < v}. Called directly they will
 * exclude a row group whose Parquet nulls such a filter would have matched. Reach them through
 * {@code StatisticsEvaluator.maybeMakeForFilter} for an answer that accounts for those rows.
 * <p>
 * A null used as a <i>bound</i> is a different question again, and is read as "the filter is unbounded at that end".
 */
final class CharPushdownHandler {

    /**
     * Resolves {@code filter} to an evaluator, or returns {@code null} if this handler does not serve it. This is the
     * entry point {@link StatisticsEvaluator} dispatches through; the typed overloads below skip the column-type test,
     * since choosing this handler already asserts the type.
     */
    @Nullable
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final WhereFilter filter) {
        if (filter instanceof CharRangeFilter) {
            return maybeCreateEvaluator((CharRangeFilter) filter);
        }
        if (filter instanceof MatchFilter) {
            final MatchFilter matchFilter = (MatchFilter) filter;
            final Class<?> columnType = matchFilter.getColumnType();
            if (columnType == char.class || columnType == Character.class) {
                return maybeCreateEvaluator(matchFilter);
            }
        }
        return null;
    }

    /**
     * Prepares the range filter for evaluation: whether the statistics range intersects the range it defines.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final CharRangeFilter charRangeFilter) {
        final char dhLower = charRangeFilter.getLower();
        final char dhUpper = charRangeFilter.getUpper();
        final boolean unboundedBelow = dhLower == QueryConstants.NULL_CHAR;
        final boolean lowerInclusive = charRangeFilter.isLowerInclusive();
        final boolean upperInclusive = charRangeFilter.isUpperInclusive();
        return statistics -> {
            final MutableObject<Character> mutableMin = new MutableObject<>();
            final MutableObject<Character> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForChars(statistics, mutableMin::setValue, mutableMax::setValue)) {
                // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
                return true;
            }
            if (unboundedBelow) {
                if (mutableMin.get() <= QueryConstants.NULL_CHAR
                        && QueryConstants.NULL_CHAR <= mutableMax.get()) {
                    // A stored value equal to the sentinel reads back as null and so matches too. Unlike a Parquet
                    // null it is an ordinary value here, covered by min/max rather than by the null gate in
                    // maybeMakeForFilter.
                    return true;
                }
                // Filter is unbounded below; can only match if the upper bound is above this row group's minimum.
                return upperInclusive ? mutableMin.get() <= dhUpper : mutableMin.get() < dhUpper;
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
    static boolean maybeOverlapsRangeImpl(
            final char min, final char max,
            final char lower, final boolean lowerInclusive,
            final char upper, final boolean upperInclusive) {
        if ((upperInclusive && lowerInclusive) ? lower > upper : lower >= upper) {
            return false; // Empty range, no overlap
        }
        // Following logic assumes (min, max) to be a continuous range and not granular. So (a,b) will be considered
        // as "maybe overlapping" with [a, b] where b follows immediately after a.
        return (upperInclusive ? min <= upper : min < upper)
                && (lowerInclusive ? max >= lower : max > lower);
    }

    /**
     * Verifies that the statistics range intersects any point provided in the match filter.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getMatchOptions().inverted();
        if (values == null || values.length == 0) {
            // No values to check against
            return invertMatch ? StatisticsEvaluator.ALWAYS_MAYBE : statistics -> false;
        }
        // Null deliberately stays among the values; see "Nulls" on this class.
        final char[] unboxedValues = ArrayTypeUtils.getUnboxedCharArray(values);
        if (invertMatch) {
            // Arrays.sort is correct here (even though NULL need not sort where Deephaven likes it): we walk
            // the *gaps* between adjacent values and test them against min/max with the same primitive comparisons,
            // so the values must be in that order rather than Deephaven's.
            Arrays.sort(unboxedValues);
        }
        return statistics -> {
            final MutableObject<Character> mutableMin = new MutableObject<>();
            final MutableObject<Character> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForChars(statistics, mutableMin::setValue, mutableMax::setValue)) {
                // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
                return true;
            }
            return invertMatch
                    ? maybeMatchesInverse(mutableMin.get(), mutableMax.get(), unboxedValues)
                    : maybeMatches(mutableMin.get(), mutableMax.get(), unboxedValues);
        };
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    static boolean maybeMatches(
            final char min,
            final char max,
            @NotNull final char[] values) {
        for (final char value : values) {
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
     * <p>
     * Gaps between adjacent values are deliberately treated as non-empty. {@code X not in (5, 6)} against statistics
     * {@code [5, 6]} reports "maybe" although no integer lies strictly between 5 and 6. Closing that would need
     * per-type successor arithmetic -- and the floating-point equivalent, where the next representable value depends on
     * the type -- for a purely performance win, in code whose failure mode is wrong results. Left as is; the tests
     * record the tighter answer in their comments.
     * 
     * where {@code ...} represents the extreme ends of the range.
     */
    static boolean maybeMatchesInverse(
            final char min,
            final char max,
            @NotNull final char[] values) {
        if (min < values[0]) {
            return true;
        }
        final int numValues = values.length;
        for (int i = 0; i < numValues - 1; i++) {
            if (maybeOverlapsRangeImpl(min, max, values[i], false, values[i + 1], false)) {
                return true;
            }
        }
        if (max > values[numValues - 1]) {
            return true;
        }
        return false;
    }
}
