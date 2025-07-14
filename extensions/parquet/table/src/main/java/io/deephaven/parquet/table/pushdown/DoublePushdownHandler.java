//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.engine.table.impl.select.DoubleRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

@InternalUseOnly
public abstract class DoublePushdownHandler {

    /**
     * Verifies that the statistics range intersects the range defined by the filter.
     */
    public static boolean maybeOverlaps(
            @NotNull final DoubleRangeFilter doubleRangeFilter,
            @NotNull final Statistics<?> statistics) {
        // Skip pushdown-based filtering for nulls and NaNs to err on the safer side instead of adding more complex handling logic.
        // TODO (DH-19666): Improve handling of nulls
        final double dhLower = doubleRangeFilter.getLower();
        final double dhUpper = doubleRangeFilter.getUpper();
        if (Double.isNaN(dhLower) || Double.isNaN(dhUpper) ||
                dhLower == QueryConstants.NULL_DOUBLE || dhUpper == QueryConstants.NULL_DOUBLE) {
            return true;
        }
        final MutableObject<Double> mutableMin = new MutableObject<>();
        final MutableObject<Double> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForDoubles(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
            return true;
        }
        return maybeOverlapsRangeImpl(
                mutableMin.getValue(), mutableMax.getValue(),
                dhLower, doubleRangeFilter.isLowerInclusive(),
                dhUpper, doubleRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsRangeImpl(
            final double min, final double max,
            final double lower, final boolean lowerInclusive,
            final double upper, final boolean upperInclusive) {
        final int c0 = Double.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = Double.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = Double.compare(min, upper);
        if (c2 > 0) {
            // min > upper, no overlap possible.
            return false;
        }
        return (c1 < 0 && c2 < 0)
                || (c1 == 0 && lowerInclusive)
                || (c2 == 0 && upperInclusive);
    }

    /**
     * Verifies that the statistics range intersects any point provided in the match filter.
     */
    public static boolean maybeOverlaps(
            @NotNull final MatchFilter matchFilter,
            @NotNull final Statistics<?> statistics) {
        final Object[] values = matchFilter.getValues();
        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        // Skip pushdown-based filtering for nulls and NaNs to err on the safer side instead of adding more complex handling logic.
        // TODO (DH-19666): Improve handling of nulls
        final double[] unboxedValues = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            final double value = TypeUtils.getUnboxedDouble(values[i]);
            if (Double.isNaN(value) || value == QueryConstants.NULL_DOUBLE) {
                return true;
            }
            unboxedValues[i] = value;
        }
        final MutableObject<Double> mutableMin = new MutableObject<>();
        final MutableObject<Double> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForDoubles(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
            return true;
        }
        if (!matchFilter.getInvertMatch()) {
            return maybeMatches(mutableMin.getValue(), mutableMax.getValue(), unboxedValues);
        }
        return maybeMatchesInverse(mutableMin.getValue(), mutableMax.getValue(), unboxedValues);
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

    /**
     * Verifies that the {@code [min, max]} range includes any value that is not in the given {@code values} array. This
     * is done by checking whether {@code [min, max]} overlaps with every open gap produced by excluding the given
     * values. For example, if the values are sorted as {@code v_0, v_1, ..., v_n-1}, then the gaps are:
     *
     * <pre>
     * [Double.NEGATIVE_INFINITY, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, Double.POSITIVE_INFINITY]
     * </pre>
     */
    private static boolean maybeMatchesInverse(
            final double min,
            final double max,
            @NotNull final double[] values) {
        Arrays.sort(values);
        double lower = Double.NEGATIVE_INFINITY;
        boolean lowerInclusive = true;
        for (final double upper : values) {
            if (maybeOverlapsRangeImpl(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlapsRangeImpl(min, max, lower, lowerInclusive, Double.POSITIVE_INFINITY, true);
    }
}
