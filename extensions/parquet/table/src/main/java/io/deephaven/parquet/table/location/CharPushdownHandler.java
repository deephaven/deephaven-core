//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.CharRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


/**
 * Applies a {@link CharRangeFilter} or a char-typed {@link MatchFilter} to one row group's {@code min}/{@code max}
 * statistics, answering whether that row group could hold a matching row.
 *
 * <h2>Usage</h2>
 *
 * Call {@link #maybeCreateEvaluator} once per filter and apply the evaluator it returns to each row group in turn.
 * Everything that depends only on the filter -- unboxing a match filter's values, reading a range filter's bounds --
 * happens in that single call rather than once per row group.
 * <p>
 * The interval arithmetic lives in {@link #maybeOverlapsRangeImpl}. {@link #maybeMatches} reuses it by testing each of
 * the filter's values as the closed range {@code [v, v]}. {@link #maybeMatchesInverse} cannot use it at all: since
 * {@code min}/{@code max} bound only the endpoints and say nothing about which values lie between them, the one case it
 * can exclude is a row group holding a single distinct value that the filter names.
 *
 * <h2>Nulls</h2>
 *
 * Of the two sources of a Deephaven null that {@link StatisticsEvaluator} describes, only the <b>stored sentinel</b> --
 * a value equal to {@code NULL_CHAR} -- is this class's business. To Parquet it is an ordinary value, sitting inside
 * {@code min}/{@code max} like any other, so the tests here account for it: a match filter keeps the sentinel among its
 * values, and a range filter with a null lower bound admits it or not according to whether that bound is held
 * inclusively -- {@code X > null} is the one shape that rules the sentinel out.
 * <p>
 * <b>These methods are not correct in isolation</b> for a filter that a null row satisfies -- {@code X == null},
 * {@code X != v}, {@code X < v}. Called directly they will exclude a row group whose Parquet nulls such a filter would
 * have matched; reach them through {@code StatisticsEvaluator.makeForFilter}, which accounts for those rows.
 */
final class CharPushdownHandler {

    /**
     * Resolves {@code filter} to an evaluator, or returns {@code null} if this handler does not serve it. This is the
     * entry point {@link StatisticsEvaluator} dispatches through; the typed overloads below skip the column-type test,
     * since choosing this handler already asserts the type.
     */
    @Nullable
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final WhereFilter filter) {
        if (filter instanceof final CharRangeFilter charRangeFilter) {
            return maybeCreateEvaluator(charRangeFilter);
        }
        if (filter instanceof final MatchFilter matchFilter) {
            final Class<?> columnType = matchFilter.getColumnType();
            if (columnType == char.class || columnType == Character.class) {
                return maybeCreateEvaluator(matchFilter);
            }
        }
        return null;
    }

    /**
     * Prepares the range filter for evaluation: whether the statistics range intersects the range it defines. Which
     * shape of range it is settled here, once, rather than re-tested for every row group.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final CharRangeFilter charRangeFilter) {
        final char dhLower = charRangeFilter.getLower();
        final char dhUpper = charRangeFilter.getUpper();
        final boolean lowerInclusive = charRangeFilter.isLowerInclusive();
        final boolean upperInclusive = charRangeFilter.isUpperInclusive();
        // region null-lower-bound
        if (dhLower == QueryConstants.NULL_CHAR) {
            // NULL_CHAR is Character.MAX_VALUE, so [NULL_CHAR, upper] is empty and the sentinel takes its own test.
            final boolean nullMatches = lowerInclusive;
            return statistics -> {
                final char[] minMax = decodeMinMax(statistics);
                if (minMax == null) {
                    // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
                    return true;
                }
                return (nullMatches
                        && minMax[0] <= QueryConstants.NULL_CHAR && QueryConstants.NULL_CHAR <= minMax[1])
                        || maybeOverlapsRangeImpl(
                                minMax[0], minMax[1],
                                QueryConstants.MIN_CHAR, true,
                                dhUpper, upperInclusive);
            };
        }
        // endregion null-lower-bound
        return statistics -> {
            final char[] minMax = decodeMinMax(statistics);
            return minMax == null || maybeOverlapsRangeImpl(
                    minMax[0], minMax[1],
                    dhLower, lowerInclusive,
                    dhUpper, upperInclusive);
        };
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsRangeImpl(
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
     * Verifies that the statistics range intersects any point provided in the match filter. Regular and inverted
     * matches are entirely different walks, so which one applies is settled here rather than per row group.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getMatchOptions().inverted();
        if (values == null || values.length == 0) {
            // No values to check against
            return invertMatch ? StatisticsEvaluator.ALWAYS_MAYBE : StatisticsEvaluator.ALWAYS_NO_OVERLAP;
        }
        // Null deliberately stays among the values; see "Nulls" on this class.
        final char[] unboxedValues = ArrayTypeUtils.getUnboxedCharArray(values);
        if (invertMatch) {
            return statistics -> {
                final char[] minMax = decodeMinMax(statistics);
                return minMax == null || maybeMatchesInverse(minMax[0], minMax[1], unboxedValues);
            };
        }
        return statistics -> {
            final char[] minMax = decodeMinMax(statistics);
            return minMax == null || maybeMatches(minMax[0], minMax[1], unboxedValues);
        };
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    private static boolean maybeMatches(
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
     * Verifies that the {@code [min, max]} range includes any value that is not in the given {@code values} array.
     */
    private static boolean maybeMatchesInverse(
            final char min,
            final char max,
            @NotNull final char[] values) {
        // A row group can only be excluded if it holds a single distinct value, AND that value is one of the
        // filter's values.
        if (min != max) {
            return true;
        }
        for (final char value : values) {
            if (value == min) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads this row group's extremes as {@code {min, max}}, or returns {@code null} if the statistics cannot be used.
     */
    @Nullable
    private static char[] decodeMinMax(@NotNull final Statistics<?> statistics) {
        final char[] minMax = new char[2];
        if (!MinMaxFromStatistics.getMinMaxForChars(statistics, v -> minMax[0] = v, v -> minMax[1] = v)) {
            return null;
        }
        return minMax;
    }
}
