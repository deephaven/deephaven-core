//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.select.FloatRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

@InternalUseOnly
public abstract class FloatPushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final MinMax<?> minMax) {
        final float min = (Float) minMax.min();
        final float max = (Float) minMax.max();
        if (filter instanceof FloatRangeFilter) {
            return maybeOverlaps(min, max, (FloatRangeFilter) filter);
        } else if (filter instanceof MatchFilter) {
            return maybeMatches(min, max, (MatchFilter) filter);
        }
        return true;
    }

    private static boolean maybeOverlaps(
            final float min,
            final float max,
            final FloatRangeFilter floatRangeFilter) {
        // Skip pushdown-based filtering for nulls and NaNs
        final float dhLower = floatRangeFilter.getLower();
        final float dhUpper = floatRangeFilter.getUpper();
        if (Float.isNaN(dhLower) || Float.isNaN(dhUpper) ||
                dhLower == QueryConstants.NULL_FLOAT || dhUpper == QueryConstants.NULL_FLOAT) {
            return true;
        }
        return maybeOverlapsImpl(
                min, max,
                dhLower, floatRangeFilter.isLowerInclusive(),
                dhUpper, floatRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsImpl(
            final float min, final float max,
            final float lower, final boolean lowerInclusive,
            final float upper, final boolean upperInclusive) {
        final int c0 = FloatComparisons.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = FloatComparisons.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = FloatComparisons.compare(min, upper);
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
            final float min,
            final float max,
            final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getInvertMatch();

        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        // Skip pushdown-based filtering for nulls and NaNs
        final float[] unboxedValues = new float[values.length];
        for (int i = 0; i < values.length; i++) {
            final float value = TypeUtils.getUnboxedFloat(values[i]);
            if (Float.isNaN(value) || value == QueryConstants.NULL_FLOAT) {
                return true;
            }
            unboxedValues[i] = value;
        }
        if (!invertMatch) {
            return maybeMatchesImpl(min, max, unboxedValues);
        }
        return maybeMatchesInverseImpl(min, max, unboxedValues);
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    private static boolean maybeMatchesImpl(
            final float min,
            final float max,
            @NotNull final float[] values) {
        for (final float value : values) {
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
     * [Float.NEGATIVE_INFINITY, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, Float.POSITIVE_INFINITY]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final float min,
            final float max,
            @NotNull final float[] values) {
        Arrays.sort(values);
        float lower = Float.NEGATIVE_INFINITY;
        boolean lowerInclusive = true;
        for (final float upper : values) {
            if (maybeOverlapsImpl(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlapsImpl(min, max, lower, lowerInclusive, Float.POSITIVE_INFINITY, true);
    }
}
