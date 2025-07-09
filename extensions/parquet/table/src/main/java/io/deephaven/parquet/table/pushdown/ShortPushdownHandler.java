//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.select.ShortRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.type.TypeUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Optional;

@InternalUseOnly
public abstract class ShortPushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final Statistics<?> statistics) {
        final Optional<MinMax<?>> minMaxFromStatistics = MinMaxFromStatistics.get(statistics, Short.class);
        if (minMaxFromStatistics.isEmpty()) {
            // Statistics could not be processed, so we cannot determine overlaps.
            return true;
        }
        final MinMax<?> minMax = minMaxFromStatistics.get();
        final short min = (Short) minMax.min();
        final short max = (Short) minMax.max();
        if (filter instanceof ShortRangeFilter) {
            return maybeOverlaps(min, max, (ShortRangeFilter) filter);
        } else if (filter instanceof MatchFilter) {
            return maybeMatches(min, max, (MatchFilter) filter);
        }
        return true;
    }

    static boolean maybeOverlaps(
            final short min,
            final short max,
            final ShortRangeFilter shortRangeFilter) {
        // Skip pushdown-based filtering for nulls
        final short dhLower = shortRangeFilter.getLower();
        final short dhUpper = shortRangeFilter.getUpper();
        if (dhLower == QueryConstants.NULL_SHORT || dhUpper == QueryConstants.NULL_SHORT) {
            return true;
        }
        return maybeOverlapsImpl(
                min, max,
                dhLower, shortRangeFilter.isLowerInclusive(),
                dhUpper, shortRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsImpl(
            final short min, final short max,
            final short lower, final boolean lowerInclusive,
            final short upper, final boolean upperInclusive) {
        final int c0 = Short.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = Short.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = Short.compare(min, upper);
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
            final short min,
            final short max,
            final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        // Skip pushdown-based filtering for nulls
        final short[] unboxedValues = new short[values.length];
        for (int i = 0; i < values.length; i++) {
            final short value = TypeUtils.getUnboxedShort(values[i]);
            if (value == QueryConstants.NULL_SHORT) {
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
            final short min,
            final short max,
            @NotNull final short[] values) {
        for (final short value : values) {
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
     * [Short.MIN_VALUE, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, Short.MAX_VALUE]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final short min,
            final short max,
            @NotNull final short[] values) {
        Arrays.sort(values);
        short lower = Short.MIN_VALUE;
        boolean lowerInclusive = true;
        for (final short upper : values) {
            if (maybeOverlapsImpl(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlapsImpl(min, max, lower, lowerInclusive, Short.MAX_VALUE, true);
    }
}
