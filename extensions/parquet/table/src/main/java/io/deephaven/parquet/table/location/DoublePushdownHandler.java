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
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

final class DoublePushdownHandler {

    /**
     * Verifies that the statistics range intersects the range defined by the filter.
     */
    static boolean maybeOverlaps(
            @NotNull final DoubleRangeFilter doubleRangeFilter,
            @NotNull final Statistics<?> statistics) {
        // Null rows are accounted for by the null guard in ParquetTableLocation.pushdownRowGroupMetadata:
        // a filter that can match null (`X < v` does) reaches this only for row groups proven to hold
        // none, and a filter that cannot match null is unaffected by their presence.
        //
        // DoubleRangeFilter's constructor orders the pair with DoubleComparisons, under which the null sentinel
        // is below every value and NaN above every one. So `lower` is the only end that can be NULL_DOUBLE, and
        // `upper` the only end that can be NaN; each marks the filter as unbounded at that end.
        final double dhLower = doubleRangeFilter.getLower();
        final double dhUpper = doubleRangeFilter.getUpper();
        final boolean filterUnboundedBelow = dhLower == QueryConstants.NULL_DOUBLE;
        final boolean filterUnboundedAbove = Double.isNaN(dhUpper);
        if (filterUnboundedBelow && filterUnboundedAbove) {
            // The filter constrains nothing at either end.
            return true;
        }
        final MutableObject<Double> mutableMin = new MutableObject<>();
        final MutableObject<Double> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForDoubles(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
            return true;
        }
        if (filterUnboundedAbove) {
            // Filter is unbounded above; can only match if the lower bound is below this row group's maximum.
            return doubleRangeFilter.isLowerInclusive()
                    ? mutableMax.get() >= dhLower
                    : mutableMax.get() > dhLower;
        }
        if (filterUnboundedBelow) {
            // Filter is unbounded below; can only match if the upper bound is above this row group's minimum.
            return doubleRangeFilter.isUpperInclusive()
                    ? mutableMin.get() <= dhUpper
                    : mutableMin.get() < dhUpper;
        }
        return maybeOverlapsRangeImpl(
                mutableMin.get(), mutableMax.get(),
                dhLower, doubleRangeFilter.isLowerInclusive(),
                dhUpper, doubleRangeFilter.isUpperInclusive());
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
     * statistics range intersects any of its values.
     * <p>
     * An <i>inverted</i> match is never served, and returns {@link StatisticsEvaluator#ALWAYS_MAYBE} at the top of this
     * method. Unlike the integral handlers there is no {@code maybeMatchesInverse} here at all, because no statistics
     * can justify excluding a row group from one -- see the note on that early return.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final MatchFilter matchFilter) {
        if (matchFilter.getMatchOptions().inverted()) {
            // NaN satisfies any inverted match, and the statistics cannot prove its absence: conforming writers
            // omit NaN from min/max. Absent that proof, the row group cannot be excluded. Everything below is
            // therefore the regular-match path only.
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        final Object[] values = matchFilter.getValues();
        if (values == null || values.length == 0) {
            // No values to check against
            return statistics -> false;
        }
        // Skip pushdown-based filtering for nulls and NaNs to err on the safer side instead of adding more complex
        // handling logic.
        // TODO (DH-19666): Improve handling of nulls
        final double[] unboxedValues = ArrayTypeUtils.getUnboxedDoubleArray(values);
        for (final double value : unboxedValues) {
            if (Double.isNaN(value) || value == QueryConstants.NULL_DOUBLE) {
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
     * Convenience for a single row group; prefer {@link #maybeCreateEvaluator} when iterating over several.
     */
    static boolean maybeOverlaps(
            @NotNull final MatchFilter matchFilter,
            @NotNull final Statistics<?> statistics) {
        return maybeCreateEvaluator(matchFilter).maybeOverlaps(statistics);
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
