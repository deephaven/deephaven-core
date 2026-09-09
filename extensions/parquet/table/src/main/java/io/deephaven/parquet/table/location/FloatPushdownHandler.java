//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.impl.select.FloatRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


/**
 * Applies a {@link FloatRangeFilter} or a float-typed {@link MatchFilter} to one row group's {@code min}/{@code max}
 * statistics, answering whether that row group could hold a matching row.
 *
 * <h2>Usage</h2>
 *
 * Call {@link #maybeCreateEvaluator} once per filter and apply the evaluator it returns to each row group in turn.
 * Everything that depends only on the filter -- unboxing a match filter's values, reading a range filter's bounds --
 * happens in that single call rather than once per row group. The interval arithmetic lives in
 * {@link #maybeOverlapsRangeImpl}, which {@link #maybeMatches} reuses by testing each of the filter's values as the
 * closed range {@code [v, v]}.
 *
 * <h2>NaN</h2>
 *
 * A conforming writer leaves NaN out of {@code min}/{@code max} entirely, so no statistics can prove a row group holds
 * none. Every filter is therefore asked first whether a NaN row could satisfy it; if so, the row group is kept.
 * <p>
 * For a match filter that follows from {@link MatchOptions#nanMatch()} and {@link MatchOptions#inverted()}, not from
 * the shape of the filter: {@code !isNaN(X)} is an inverted match that no NaN row satisfies.
 * <p>
 * For a range filter it is the upper bound. An <i>inclusive</i> NaN upper matches the NaN rows, since
 * {@link io.deephaven.util.compare.FloatComparisons} places NaN above every value and equal to itself. Of the factories
 * only {@code leq(col, NaN)} builds one, and it matches every row anyway; the exclusive NaN upper that {@code gt} and
 * {@code geq} produce still reads as "unbounded above" and prunes as before.
 *
 * <h2>Nulls</h2>
 *
 * Of the two sources of a Deephaven null that {@link StatisticsEvaluator} describes, only the <b>stored sentinel</b> --
 * a value equal to {@code NULL_FLOAT} -- is this class's business. To Parquet it is an ordinary value, sitting inside
 * {@code min}/{@code max} like any other, so the tests here account for it: a match filter keeps the sentinel among its
 * values, and a range filter whose lower bound is the sentinel looks for it explicitly when that bound is held
 * inclusively. {@code X > null}, holding it exclusively, matches no null row, but the handler answers conservatively
 * rather than test for the one row group shape that could exploit that -- nothing but sentinels.
 * <p>
 * <b>These methods are not correct in isolation</b> for a filter that a null row satisfies -- {@code X == null},
 * {@code X < v}. Called directly they will exclude a row group whose Parquet nulls such a filter would have matched;
 * reach them through {@code StatisticsEvaluator.makeForFilter}, which accounts for those rows.
 * <p>
 * Note that {@code NULL_FLOAT} is not the bottom of the domain: the infinities lie outside it. A row group holding
 * negative infinity therefore brackets the sentinel, and {@code X == null} declines to exclude that row group. The
 * answer is still correct, and this is the only case where the sentinel costs pruning on a Deephaven-written file,
 * which can contain no stored sentinel at all.
 */
final class FloatPushdownHandler {

    /**
     * Resolves {@code filter} to an evaluator, or returns {@code null} if this handler does not serve it. This is the
     * entry point {@link StatisticsEvaluator} dispatches through; the typed overloads below skip the column-type test,
     * since choosing this handler already asserts the type.
     */
    @Nullable
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final WhereFilter filter) {
        if (filter instanceof final FloatRangeFilter floatRangeFilter) {
            return maybeCreateEvaluator(floatRangeFilter);
        }
        if (filter instanceof final MatchFilter matchFilter) {
            final Class<?> columnType = matchFilter.getColumnType();
            if (columnType == float.class || columnType == Float.class) {
                return maybeCreateEvaluator(matchFilter);
            }
        }
        return null;
    }

    /**
     * Prepares the range filter for evaluation: whether the statistics range intersects the range it defines. A filter
     * a NaN row could satisfy resolves to {@link StatisticsEvaluator#ALWAYS_MAYBE} here -- no statistics can rule such
     * rows out -- so the caller can skip the row groups rather than ask about each in turn.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final FloatRangeFilter floatRangeFilter) {
        // FloatRangeFilter's constructor orders the pair with FloatComparisons, under which the null sentinel
        // is below every value and NaN above every one. So `lower` is the only end that can be NULL_FLOAT, and
        // `upper` the only end that can be NaN.
        final float lower = floatRangeFilter.getLower();
        final boolean lowerInclusive = floatRangeFilter.isLowerInclusive();
        final float upper;
        final boolean upperInclusive;
        if (Float.isNaN(floatRangeFilter.getUpper())) {
            if (floatRangeFilter.isUpperInclusive()) {
                // An inclusive NaN upper matches the NaN rows, which no statistics can rule out.
                return StatisticsEvaluator.ALWAYS_MAYBE;
            }
            // Every comparison against NaN is false, so NaN cannot feed the interval test. Statistics never contain
            // NaN, so an inclusive MAX_FLOAT upper is equivalent.
            upper = QueryConstants.MAX_FLOAT;
            upperInclusive = true;
        } else {
            upper = floatRangeFilter.getUpper();
            upperInclusive = floatRangeFilter.isUpperInclusive();
        }
        if (lower != QueryConstants.NULL_FLOAT) {
            // Both bounds are ordinary values; the plain interval test decides. A -Inf lower counts the sentinel as
            // an ordinary in-range value, but this filter matches no null row, so that can only over-keep -- which
            // "maybe" allows.
            return statistics -> {
                final float[] minMax = decodeMinMax(statistics);
                return minMax == null || maybeOverlapsRangeImpl(
                        minMax[0], minMax[1],
                        lower, lowerInclusive,
                        upper, upperInclusive);
            };
        }
        return statistics -> {
            final float[] minMax = decodeMinMax(statistics);
            // An inclusive null bound admits the sentinel wherever the stats span it -- the interval test misses it
            // only when `upper` is -Inf. The interval test settles the ordinary values.
            return minMax == null
                    || (lowerInclusive
                            && minMax[0] <= QueryConstants.NULL_FLOAT && QueryConstants.NULL_FLOAT <= minMax[1])
                    || maybeOverlapsRangeImpl(
                            minMax[0], minMax[1],
                            QueryConstants.MIN_FLOAT, true,
                            upper, upperInclusive);
        };
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds. This
     * method assumes that the caller would filter NaN values.
     * <p>
     * The comparisons below are the raw operators, which hold {@code -0.0} and {@code 0.0} to be equal. That is
     * <i>not</i> {@link Float#compare}, which separates them, but it is
     * {@link io.deephaven.util.compare.FloatComparisons}, whose {@code compare} makes them equal deliberately -- so
     * this agrees with how the engine orders the values it will go on to filter.
     */
    private static boolean maybeOverlapsRangeImpl(
            final float min, final float max,
            final float lower, final boolean lowerInclusive,
            final float upper, final boolean upperInclusive) {
        // Following logic assumes (min, max) to be a continuous range and not granular. So (a,b) will be considered
        // as "maybe overlapping" with [a, b] where b follows immediately after a.
        if ((upperInclusive && lowerInclusive) ? lower > upper : lower >= upper) {
            return false; // Empty range, no overlap
        }
        return (upperInclusive ? min <= upper : min < upper)
                && (lowerInclusive ? max >= lower : max > lower);
    }

    /**
     * Prepares the match filter for evaluation against row group statistics. A filter a NaN row could satisfy keeps
     * every row group; otherwise the ordinary walk applies, a NaN among the values being inert there.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final MatchFilter matchFilter) {
        final MatchOptions matchOptions = matchFilter.getMatchOptions();
        final boolean invertMatch = matchOptions.inverted();
        final Object[] values = matchFilter.getValues();
        if (values == null || values.length == 0) {
            // No values to check against
            return invertMatch ? StatisticsEvaluator.ALWAYS_MAYBE : StatisticsEvaluator.ALWAYS_NO_OVERLAP;
        }
        // NULL_FLOAT deliberately stays among the values; see "Nulls" on this class.
        final float[] unboxedValues = ArrayTypeUtils.getUnboxedFloatArray(values);
        int numNaN = 0;
        for (final float value : unboxedValues) {
            if (Float.isNaN(value)) {
                numNaN++;
            }
        }
        // A NaN row matches a value only if that value is NaN and nanMatch is true.
        final boolean nanRowMatchesAValue = matchOptions.nanMatch() && numNaN > 0;
        if (invertMatch ? !nanRowMatchesAValue : nanRowMatchesAValue) {
            // A NaN row satisfies this filter, and no statistics can prove the row group holds none.
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        if (numNaN == unboxedValues.length) {
            // Nothing but NaN in the value set, and NaN is already handled. The content of the row group
            // doesn't even matter.
            return invertMatch ? StatisticsEvaluator.ALWAYS_MAYBE : StatisticsEvaluator.ALWAYS_NO_OVERLAP;
        }
        // Remaining NaN among the values are inert. Could remove them but no reason.
        if (invertMatch) {
            return statistics -> {
                final float[] minMax = decodeMinMax(statistics);
                return minMax == null || maybeMatchesInverse(minMax[0], minMax[1], unboxedValues);
            };
        }
        return statistics -> {
            final float[] minMax = decodeMinMax(statistics);
            return minMax == null || maybeMatches(minMax[0], minMax[1], unboxedValues);
        };
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    private static boolean maybeMatches(
            final float min,
            final float max,
            @NotNull final float[] values) {
        for (final float value : values) {
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
            final float min,
            final float max,
            @NotNull final float[] values) {
        // A row group can only be excluded if it holds a single distinct value, AND that value is one of the
        // filter's values.
        if (min != max) {
            return true;
        }
        for (final float value : values) {
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
    private static float[] decodeMinMax(@NotNull final Statistics<?> statistics) {
        final float[] minMax = new float[2];
        if (!MinMaxFromStatistics.getMinMaxForFloats(statistics, v -> minMax[0] = v, v -> minMax[1] = v)) {
            return null;
        }
        return minMax;
    }
}
