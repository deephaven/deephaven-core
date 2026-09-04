//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


/**
 * Applies a {@link LongRangeFilter} or a long-typed {@link MatchFilter} to one row group's {@code min}/{@code max}
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
 * a value equal to {@code NULL_LONG} -- is this class's business. To Parquet it is an ordinary value, sitting inside
 * {@code min}/{@code max} like any other, so the tests here account for it: a match filter keeps the sentinel among its
 * values, and a range filter with a null lower bound admits it or not according to whether that bound is held
 * inclusively -- {@code X > null} is the one shape that rules the sentinel out.
 * <p>
 * <b>These methods are not correct in isolation</b> for a filter that a null row satisfies -- {@code X == null},
 * {@code X != v}, {@code X < v}. Called directly they will exclude a row group whose Parquet nulls such a filter would
 * have matched; reach them through {@code StatisticsEvaluator.makeForFilter}, which accounts for those rows.
 */
final class LongPushdownHandler {

    /**
     * Resolves {@code filter} to an evaluator, or returns {@code null} if this handler does not serve it. This is the
     * entry point {@link StatisticsEvaluator} dispatches through; the typed overloads below skip the column-type test,
     * since choosing this handler already asserts the type.
     */
    @Nullable
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final WhereFilter filter) {
        if (filter instanceof final LongRangeFilter longRangeFilter) {
            return maybeCreateEvaluator(longRangeFilter);
        }
        if (filter instanceof final MatchFilter matchFilter) {
            final Class<?> columnType = matchFilter.getColumnType();
            if (columnType == long.class || columnType == Long.class) {
                return maybeCreateEvaluator(matchFilter);
            }
        }
        return null;
    }

    /**
     * Prepares the range filter for evaluation: whether the statistics range intersects the range it defines. Which
     * shape of range it is settled here, once, rather than re-tested for every row group.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final LongRangeFilter longRangeFilter) {
        final long dhLower = longRangeFilter.getLower();
        final long dhUpper = longRangeFilter.getUpper();
        final boolean lowerInclusive = longRangeFilter.isLowerInclusive();
        final boolean upperInclusive = longRangeFilter.isUpperInclusive();
        // region null-lower-bound
        // A null lower bound needs no reading of its own: the sentinel is MIN_VALUE, the domain's bottom.
        // endregion null-lower-bound
        return statistics -> {
            final long[] minMax = decodeMinMax(statistics);
            return minMax == null || maybeOverlapsRangeImpl(
                    minMax[0], minMax[1],
                    dhLower, lowerInclusive,
                    dhUpper, upperInclusive);
        };
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    // Package-accessible, unlike the other replicas: Instant bounds are epoch nanoseconds, so InstantPushdownHandler
    // reuses this arithmetic rather than duplicating it. See ReplicateParquetPushdownHandlers.
    static boolean maybeOverlapsRangeImpl(
            final long min, final long max,
            final long lower, final boolean lowerInclusive,
            final long upper, final boolean upperInclusive) {
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
        final long[] unboxedValues = ArrayTypeUtils.getUnboxedLongArray(values);
        if (invertMatch) {
            return statistics -> {
                final long[] minMax = decodeMinMax(statistics);
                return minMax == null || maybeMatchesInverse(minMax[0], minMax[1], unboxedValues);
            };
        }
        return statistics -> {
            final long[] minMax = decodeMinMax(statistics);
            return minMax == null || maybeMatches(minMax[0], minMax[1], unboxedValues);
        };
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    // Package-accessible, unlike the other replicas: Instant bounds are epoch nanoseconds, so InstantPushdownHandler
    // reuses this arithmetic rather than duplicating it. See ReplicateParquetPushdownHandlers.
    static boolean maybeMatches(
            final long min,
            final long max,
            @NotNull final long[] values) {
        for (final long value : values) {
            if (maybeOverlapsRangeImpl(min, max, value, true, value, true)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Verifies that the {@code [min, max]} range includes any value that is not in the given {@code values} array.
     */
    // Package-accessible, unlike the other replicas: Instant bounds are epoch nanoseconds, so InstantPushdownHandler
    // reuses this arithmetic rather than duplicating it. See ReplicateParquetPushdownHandlers.
    static boolean maybeMatchesInverse(
            final long min,
            final long max,
            @NotNull final long[] values) {
        // A row group can only be excluded if it holds a single distinct value, AND that value is one of the
        // filter's values.
        if (min != max) {
            return true;
        }
        for (final long value : values) {
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
    private static long[] decodeMinMax(@NotNull final Statistics<?> statistics) {
        final long[] minMax = new long[2];
        if (!MinMaxFromStatistics.getMinMaxForLongs(statistics, v -> minMax[0] = v, v -> minMax[1] = v)) {
            return null;
        }
        return minMax;
    }
}
