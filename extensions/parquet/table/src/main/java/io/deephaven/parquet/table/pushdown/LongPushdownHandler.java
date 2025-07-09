//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

@InternalUseOnly
public abstract class LongPushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final MinMax<?> minMax) {
        final long min = (Long) minMax.min();
        final long max = (Long) minMax.max();
        if (filter instanceof LongRangeFilter) {
            return maybeOverlaps(min, max, (LongRangeFilter) filter);
        } else if (filter instanceof MatchFilter) {
            return maybeMatches(min, max, (MatchFilter) filter);
        }
        return true;
    }

    static boolean maybeOverlaps(
            final long min,
            final long max,
            final LongRangeFilter longRangeFilter) {
        // Skip pushdown-based filtering for nulls
        final long dhLower = longRangeFilter.getLower();
        final long dhUpper = longRangeFilter.getUpper();
        if (dhLower == QueryConstants.NULL_LONG || dhUpper == QueryConstants.NULL_LONG) {
            return true;
        }
        return maybeOverlapsImpl(
                min, max,
                dhLower, longRangeFilter.isLowerInclusive(),
                dhUpper, longRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsImpl(
            final long min, final long max,
            final long lower, final boolean lowerInclusive,
            final long upper, final boolean upperInclusive) {
        final int c0 = Long.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = Long.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = Long.compare(min, upper);
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
            final long min,
            final long max,
            final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getInvertMatch();

        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        // Skip pushdown-based filtering for nulls
        final long[] unboxedValues = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            final long value = TypeUtils.getUnboxedLong(values[i]);
            if (value == QueryConstants.NULL_LONG) {
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
            final long min,
            final long max,
            @NotNull final long[] values) {
        for (final long value : values) {
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
     * [Long.MIN_VALUE, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, Long.MAX_VALUE]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final long min,
            final long max,
            @NotNull final long[] values) {
        Arrays.sort(values);
        long lower = Long.MIN_VALUE;
        boolean lowerInclusive = true;
        for (final long upper : values) {
            if (maybeOverlapsImpl(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlapsImpl(min, max, lower, lowerInclusive, Long.MAX_VALUE, true);
    }
}
