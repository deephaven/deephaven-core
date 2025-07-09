//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.select.ByteRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.type.TypeUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Optional;

@InternalUseOnly
public abstract class BytePushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final Statistics<?> statistics) {
        final Optional<MinMax<?>> minMaxFromStatistics = MinMaxFromStatistics.get(statistics, Byte.class);
        if (minMaxFromStatistics.isEmpty()) {
            // Statistics could not be processed, so we cannot determine overlaps.
            return true;
        }
        final MinMax<?> minMax = minMaxFromStatistics.get();
        final byte min = (Byte) minMax.min();
        final byte max = (Byte) minMax.max();
        if (filter instanceof ByteRangeFilter) {
            return maybeOverlaps(min, max, (ByteRangeFilter) filter);
        } else if (filter instanceof MatchFilter) {
            return maybeMatches(min, max, (MatchFilter) filter);
        }
        return true;
    }

    static boolean maybeOverlaps(
            final byte min,
            final byte max,
            final ByteRangeFilter byteRangeFilter) {
        // Skip pushdown-based filtering for nulls
        final byte dhLower = byteRangeFilter.getLower();
        final byte dhUpper = byteRangeFilter.getUpper();
        if (dhLower == QueryConstants.NULL_BYTE || dhUpper == QueryConstants.NULL_BYTE) {
            return true;
        }
        return maybeOverlapsImpl(
                min, max,
                dhLower, byteRangeFilter.isLowerInclusive(),
                dhUpper, byteRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsImpl(
            final byte min, final byte max,
            final byte lower, final boolean lowerInclusive,
            final byte upper, final boolean upperInclusive) {
        final int c0 = Byte.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = Byte.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = Byte.compare(min, upper);
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
            final byte min,
            final byte max,
            final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        // Skip pushdown-based filtering for nulls
        final byte[] unboxedValues = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            final byte value = TypeUtils.getUnboxedByte(values[i]);
            if (value == QueryConstants.NULL_BYTE) {
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
            final byte min,
            final byte max,
            @NotNull final byte[] values) {
        for (final byte value : values) {
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
     * [Byte.MIN_VALUE, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, Byte.MAX_VALUE]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final byte min,
            final byte max,
            @NotNull final byte[] values) {
        Arrays.sort(values);
        byte lower = Byte.MIN_VALUE;
        boolean lowerInclusive = true;
        for (final byte upper : values) {
            if (maybeOverlapsImpl(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlapsImpl(min, max, lower, lowerInclusive, Byte.MAX_VALUE, true);
    }
}
