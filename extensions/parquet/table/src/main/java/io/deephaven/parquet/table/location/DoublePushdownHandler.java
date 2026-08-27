//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.DoubleRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Applies a {@link DoubleRangeFilter} or a double-typed {@link MatchFilter} to one row group's {@code min}/{@code max}
 * statistics, answering whether that row group could hold a matching row.
 *
 * <h2>Usage</h2>
 *
 * For a match filter, {@link #maybeCreateEvaluator} resolves the filter once and returns an evaluator to apply to each
 * row group in turn. {@link #maybeOverlaps(MatchFilter, Statistics)} wraps both steps for a single row group;
 * {@link #maybeOverlaps(DoubleRangeFilter, Statistics)} serves range filters. The interval arithmetic lives in
 * {@link #maybeOverlapsRangeImpl}, which {@link #maybeMatches} reuses by testing each of the filter's values as the
 * closed range {@code [v, v]}.
 *
 * <h2>NaN</h2>
 *
 * A conforming writer leaves NaN out of {@code min}/{@code max} entirely, so no statistics can prove a row group holds
 * none. Every NaN row satisfies an inverted match, which is why -- unlike the integral handlers -- there is no
 * {@code maybeMatchesInverse} here and an inverted match is declined outright. A NaN among a regular match's values is
 * declined for the same reason: the statistics cannot place it.
 *
 * <h2>Nulls</h2>
 *
 * {@link StatisticsEvaluator} describes the two reasons a row can read back as null in Deephaven. This class answers
 * for one of them and not the other.
 * <ul>
 * <li>A <b>stored sentinel</b> -- a value equal to {@code NULL_DOUBLE} -- is this class's business. To Parquet it is an
 * ordinary value, sitting inside {@code min}/{@code max} like any other, so the tests here account for it: a match
 * filter keeps the sentinel among its values, and an unbounded-below range filter looks for it explicitly.</li>
 * <li>A <b>Parquet null</b> is not. Such a row is invisible to {@code min}/{@code max}, so nothing here can see one or
 * rule one out. {@code StatisticsEvaluator.maybeMakeForFilter} gates on that before any of this runs.</li>
 * </ul>
 * These methods therefore answer from {@code min}/{@code max} alone, and are <b>not</b> correct in isolation for a
 * filter that a null row satisfies -- {@code X == null}, {@code X < v}. Called directly they will exclude a row group
 * whose Parquet nulls such a filter would have matched. Reach them through
 * {@code StatisticsEvaluator.maybeMakeForFilter} for an answer that accounts for those rows.
 * <p>
 * Note that {@code NULL_DOUBLE} is not the bottom of the domain: the infinities lie outside it. A row group holding
 * negative infinity therefore brackets the sentinel, and {@code X == null} declines to exclude that row group. The
 * answer is still correct, and this is the only case where the sentinel costs pruning on a Deephaven-written file,
 * which can contain no stored sentinel at all.
 */
final class DoublePushdownHandler {

    /**
     * Resolves {@code filter} to an evaluator, or returns {@code null} if this handler does not serve it. This is the
     * entry point {@link StatisticsEvaluator} dispatches through; the typed overloads below skip the column-type test,
     * since choosing this handler already asserts the type.
     */
    @Nullable
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final WhereFilter filter) {
        if (filter instanceof DoubleRangeFilter) {
            return maybeCreateEvaluator((DoubleRangeFilter) filter);
        }
        if (filter instanceof MatchFilter) {
            final MatchFilter matchFilter = (MatchFilter) filter;
            final Class<?> columnType = matchFilter.getColumnType();
            if (columnType == double.class || columnType == Double.class) {
                return maybeCreateEvaluator(matchFilter);
            }
        }
        return null;
    }

    /**
     * Prepares the range filter for evaluation: whether the statistics range intersects the range it defines. A filter
     * that constrains nothing at either end resolves to {@link StatisticsEvaluator#ALWAYS_MAYBE} here, so the caller
     * can skip the row groups rather than ask about each in turn.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final DoubleRangeFilter doubleRangeFilter) {
        // DoubleRangeFilter's constructor orders the pair with DoubleComparisons, under which the null sentinel
        // is below every value and NaN above every one. So `lower` is the only end that can be NULL_DOUBLE, and
        // `upper` the only end that can be NaN; each marks the filter as unbounded at that end.
        final double dhLower = doubleRangeFilter.getLower();
        final double dhUpper = doubleRangeFilter.getUpper();
        final boolean filterUnboundedBelow = dhLower == QueryConstants.NULL_DOUBLE;
        final boolean filterUnboundedAbove = Double.isNaN(dhUpper);
        if (filterUnboundedBelow && filterUnboundedAbove) {
            // The filter constrains nothing at either end.
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        final boolean lowerInclusive = doubleRangeFilter.isLowerInclusive();
        final boolean upperInclusive = doubleRangeFilter.isUpperInclusive();
        return statistics -> {
            final MutableObject<Double> mutableMin = new MutableObject<>();
            final MutableObject<Double> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForDoubles(statistics, mutableMin::setValue, mutableMax::setValue)) {
                // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
                return true;
            }
            if (filterUnboundedAbove) {
                // Filter is unbounded above; can only match if the lower bound is below this row group's maximum.
                return lowerInclusive ? mutableMax.get() >= dhLower : mutableMax.get() > dhLower;
            }
            if (filterUnboundedBelow) {
                if (mutableMin.get() <= QueryConstants.NULL_DOUBLE
                        && QueryConstants.NULL_DOUBLE <= mutableMax.get()) {
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
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds. This
     * method assumes that the caller would filter NaN values. Also, this method is lenient towards -0.0 / 0.0
     * comparisons, when compared to {@link Double#compare}
     */
    private static boolean maybeOverlapsRangeImpl(
            final double min, final double max,
            final double lower, final boolean lowerInclusive,
            final double upper, final boolean upperInclusive) {
        // Following logic assumes (min, max) to be a continuous range and not granular. So (a,b) will be considered
        // as "maybe overlapping" with [a, b] where b follows immediately after a.
        if ((upperInclusive && lowerInclusive) ? lower > upper : lower >= upper) {
            return false; // Empty range, no overlap
        }
        return (upperInclusive ? min <= upper : min < upper)
                && (lowerInclusive ? max >= lower : max > lower);
    }

    /**
     * Prepares the match filter for evaluation against row group statistics: for a regular match, whether the
     * statistics range intersects any of its values. An inverted match is declined here; see "NaN" on this class.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final MatchFilter matchFilter) {
        if (matchFilter.getMatchOptions().inverted()) {
            // Everything below is therefore the regular-match path only.
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        final Object[] values = matchFilter.getValues();
        if (values == null || values.length == 0) {
            // No values to check against
            return statistics -> false;
        }
        // Null deliberately stays among the values; see "Nulls" on this class.
        final double[] unboxedValues = ArrayTypeUtils.getUnboxedDoubleArray(values);
        for (final double value : unboxedValues) {
            if (Double.isNaN(value)) {
                return StatisticsEvaluator.ALWAYS_MAYBE;
            }
        }
        return statistics -> {
            final MutableObject<Double> mutableMin = new MutableObject<>();
            final MutableObject<Double> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForDoubles(statistics, mutableMin::setValue, mutableMax::setValue)) {
                // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
                return true;
            }
            return maybeMatches(mutableMin.get(), mutableMax.get(), unboxedValues);
        };
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    private static boolean maybeMatches(
            final double min,
            final double max,
            @NotNull final double[] values) {
        for (final double value : values) {
            if (maybeOverlapsRangeImpl(min, max, value, true, value, true)) {
                return true;
            }
        }
        return false;
    }
}
