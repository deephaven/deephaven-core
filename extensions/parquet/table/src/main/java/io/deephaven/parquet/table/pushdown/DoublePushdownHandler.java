//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.select.DoubleRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.type.TypeUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Optional;

@InternalUseOnly
public abstract class DoublePushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final Statistics<?> statistics) {
        final Optional<MinMax<?>> minMaxFromStatistics = MinMaxFromStatistics.get(statistics, Double.class);
        if (minMaxFromStatistics.isEmpty()) {
            // Statistics could not be processed, so we cannot determine overlaps.
            return true;
        }
        final MinMax<?> minMax = minMaxFromStatistics.get();
        final double min = (Double) minMax.min();
        final double max = (Double) minMax.max();
        if (filter instanceof DoubleRangeFilter) {
            return maybeOverlaps(min, max, (DoubleRangeFilter) filter);
        } else if (filter instanceof MatchFilter) {
            return maybeMatches(min, max, (MatchFilter) filter);
        }
        return true;
    }

    private static boolean maybeOverlaps(
            final double min,
            final double max,
            final DoubleRangeFilter doubleRangeFilter) {
        // Skip pushdown-based filtering for nulls and NaNs
        final double dhLower = doubleRangeFilter.getLower();
        final double dhUpper = doubleRangeFilter.getUpper();
        if (Double.isNaN(dhLower) || Double.isNaN(dhUpper) ||
                dhLower == QueryConstants.NULL_DOUBLE || dhUpper == QueryConstants.NULL_DOUBLE) {
            return true;
        }
        return maybeOverlapsImpl(
                min, max,
                dhLower, doubleRangeFilter.isLowerInclusive(),
                dhUpper, doubleRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsImpl(
            final double min, final double max,
            final double lower, final boolean lowerInclusive,
            final double upper, final boolean upperInclusive) {
        final int c0 = DoubleComparisons.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = DoubleComparisons.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = DoubleComparisons.compare(min, upper);
        if (c2 > 0) {
            // min > upper, no overlap possible.
            return false;
        }
        return (c1 < 0 && c2 < 0)
                || (c1 == 0 && lowerInclusive)
                || (c2 == 0 && upperInclusive);
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in the filter.
     */
    private static boolean maybeMatches(
            final double min,
            final double max,
            final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        // Skip pushdown-based filtering for nulls and NaNs
        final double[] unboxedValues = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            final double value = TypeUtils.getUnboxedDouble(values[i]);
            if (Double.isNaN(value) || value == QueryConstants.NULL_DOUBLE) {
                return true;
            }
            unboxedValues[i] = value;
        }
        if (!matchFilter.getInvertMatch()) {
            return maybeMatchesImpl(min, max, unboxedValues);
        }
        return maybeMatchesInverseImpl(min, max, unboxedValues);
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    private static boolean maybeMatchesImpl(
            final double min,
            final double max,
            @NotNull final double[] values) {
        for (final double value : values) {
            if (maybeOverlapsImpl(min, max, value, true, value, true)) {
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
     * [Double.NEGATIVE_INFINITY, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, Double.POSITIVE_INFINITY]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final double min,
            final double max,
            @NotNull final double[] values) {
        Arrays.sort(values);
        double lower = Double.NEGATIVE_INFINITY;
        boolean lowerInclusive = true;
        for (final double upper : values) {
            if (maybeOverlapsImpl(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlapsImpl(min, max, lower, lowerInclusive, Double.POSITIVE_INFINITY, true);
    }
}
