//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.FloatRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
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
 * none. Every NaN row satisfies an inverted match, which is why -- unlike the integral handlers -- there is no
 * {@code maybeMatchesInverse} here: an inverted match keeps every row group instead. A regular match with a NaN among
 * its values keeps every row group for the same reason, the statistics being unable to place it.
 * <p>
 * A range filter whose upper bound is an <i>inclusive</i> NaN matches the NaN rows too, so it also keeps every row
 * group. That it matches is {@link io.deephaven.util.compare.FloatComparisons} semantics -- the ordering the engine
 * itself filters and sorts by -- in which NaN sits above every value and compares equal to itself, so the chunk filter
 * built for the range admits those rows.
 * <p>
 * Keeping every row group in that case gives up no pruning that was available before, because nothing in the query path
 * produces such a bound. The {@code lt}, {@code leq}, {@code gt} and {@code geq} factories are the source of every
 * range filter a parsed query, a client, or the UI builds, and all four make a NaN upper bound <b>exclusive</b> --
 * deliberately, so that the results omit NaN. An exclusive NaN upper bound still reads as "unbounded above" here and
 * prunes exactly as it did before; only a filter built directly through a public constructor can present an inclusive
 * one.
 *
 * <h2>Nulls</h2>
 *
 * Of the two sources of a Deephaven null that {@link StatisticsEvaluator} describes, only the <b>stored sentinel</b> --
 * a value equal to {@code NULL_FLOAT} -- is this class's business. To Parquet it is an ordinary value, sitting inside
 * {@code min}/{@code max} like any other, so the tests here account for it: a match filter keeps the sentinel among its
 * values, and an unbounded-below range filter looks for it explicitly.
 * <p>
 * <b>These methods are not correct in isolation</b> for a filter that a null row satisfies -- {@code X == null},
 * {@code X < v}. Called directly they will exclude a row group whose Parquet nulls such a filter would have matched;
 * reach them through {@code StatisticsEvaluator.maybeMakeForFilter}, which gates on those rows.
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
        if (filter instanceof FloatRangeFilter) {
            return maybeCreateEvaluator((FloatRangeFilter) filter);
        }
        if (filter instanceof MatchFilter) {
            final MatchFilter matchFilter = (MatchFilter) filter;
            final Class<?> columnType = matchFilter.getColumnType();
            if (columnType == float.class || columnType == Float.class) {
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
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final FloatRangeFilter floatRangeFilter) {
        // FloatRangeFilter's constructor orders the pair with FloatComparisons, under which the null sentinel
        // is below every value and NaN above every one. So `lower` is the only end that can be NULL_FLOAT, and
        // `upper` the only end that can be NaN.
        final float dhLower = floatRangeFilter.getLower();
        final float dhUpper = floatRangeFilter.getUpper();
        final boolean lowerInclusive = floatRangeFilter.isLowerInclusive();
        final boolean upperInclusive = floatRangeFilter.isUpperInclusive();
        if (Float.isNaN(dhUpper) && upperInclusive) {
            // Only an *exclusive* NaN upper bound means "unbounded above". Held inclusively it matches the NaN rows
            // themselves, since FloatComparisons.leq(NaN, NaN) holds, and no statistics can prove a row group holds
            // none of them. The lt/leq/gt/geq factories -- and so every range filter a parsed query, a client, or the
            // UI produces -- always make the bound exclusive; the public constructors do not, so such a filter keeps
            // every row group here rather than being read as unbounded above.
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        final boolean filterUnboundedBelow = dhLower == QueryConstants.NULL_FLOAT;
        // Exclusive past the check above, so it rules out the NaN rows and constrains nothing else.
        final boolean filterUnboundedAbove = Float.isNaN(dhUpper);
        if (filterUnboundedBelow && filterUnboundedAbove) {
            // The filter constrains nothing at either end.
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        return statistics -> {
            final MutableObject<Float> mutableMin = new MutableObject<>();
            final MutableObject<Float> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForFloats(statistics, mutableMin::setValue, mutableMax::setValue)) {
                // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
                return true;
            }
            if (filterUnboundedAbove) {
                // Filter is unbounded above; can only match if the lower bound is below this row group's maximum.
                return lowerInclusive ? mutableMax.get() >= dhLower : mutableMax.get() > dhLower;
            }
            if (filterUnboundedBelow) {
                if (mutableMin.get() <= QueryConstants.NULL_FLOAT
                        && QueryConstants.NULL_FLOAT <= mutableMax.get()) {
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
     * comparisons, when compared to {@link Float#compare}
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
     * Prepares the match filter for evaluation against row group statistics: for a regular match, whether the
     * statistics range intersects any of its values. An inverted match keeps every row group; see "NaN" on this class.
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
        final float[] unboxedValues = ArrayTypeUtils.getUnboxedFloatArray(values);
        for (final float value : unboxedValues) {
            if (Float.isNaN(value)) {
                return StatisticsEvaluator.ALWAYS_MAYBE;
            }
        }
        return statistics -> {
            final MutableObject<Float> mutableMin = new MutableObject<>();
            final MutableObject<Float> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForFloats(statistics, mutableMin::setValue, mutableMax::setValue)) {
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
}
