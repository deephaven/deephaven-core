//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.select.IntRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

@InternalUseOnly
public abstract class IntPushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final MinMax<?> minMax) {
        final int min = (Integer) minMax.min();
        final int max = (Integer) minMax.max();
        if (filter instanceof IntRangeFilter) {
            return maybeOverlaps(min, max, (IntRangeFilter) filter);
        } else if (filter instanceof MatchFilter) {
            return maybeMatches(min, max, (MatchFilter) filter);
        }
        return true;
    }

    static boolean maybeOverlaps(
            final int min,
            final int max,
            final IntRangeFilter intRangeFilter) {
        // Skip pushdown-based filtering for nulls
        final int dhLower = intRangeFilter.getLower();
        final int dhUpper = intRangeFilter.getUpper();
        if (dhLower == QueryConstants.NULL_INT || dhUpper == QueryConstants.NULL_INT) {
            return true;
        }
        return maybeOverlapsImpl(
                min, max,
                dhLower, intRangeFilter.isLowerInclusive(),
                dhUpper, intRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsImpl(
            final int min, final int max,
            final int lower, final boolean lowerInclusive,
            final int upper, final boolean upperInclusive) {
        final int c0 = Integer.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = Integer.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = Integer.compare(min, upper);
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
            final int min,
            final int max,
            final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getInvertMatch();

        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        // Skip pushdown-based filtering for nulls
        final int[] unboxedValues = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            final int value = TypeUtils.getUnboxedInt(values[i]);
            if (value == QueryConstants.NULL_INT) {
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
            final int min,
            final int max,
            @NotNull final int[] values) {
        for (final int value : values) {
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
     * [Integer.MIN_VALUE, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, Integer.MAX_VALUE]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final int min,
            final int max,
            @NotNull final int[] values) {
        Arrays.sort(values);
        int lower = Integer.MIN_VALUE;
        boolean lowerInclusive = true;
        for (final int upper : values) {
            if (maybeOverlapsImpl(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlapsImpl(min, max, lower, lowerInclusive, Integer.MAX_VALUE, true);
    }
}
