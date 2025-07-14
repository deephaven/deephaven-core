//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

final class LongPushdownHandler {

    /**
     * Verifies that the statistics range intersects the range defined by the filter.
     */
    static boolean maybeOverlaps(
            @NotNull final LongRangeFilter longRangeFilter,
            @NotNull final Statistics<?> statistics) {
        // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex handling
        // logic.
        // TODO (DH-19666): Improve handling of nulls
        final long dhLower = longRangeFilter.getLower();
        final long dhUpper = longRangeFilter.getUpper();
        if (dhLower == QueryConstants.NULL_LONG || dhUpper == QueryConstants.NULL_LONG) {
            return true;
        }
        final MutableObject<Long> mutableMin = new MutableObject<>();
        final MutableObject<Long> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForLongs(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
            return true;
        }
        return maybeOverlapsRangeImpl(
                mutableMin.getValue(), mutableMax.getValue(),
                dhLower, longRangeFilter.isLowerInclusive(),
                dhUpper, longRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    static boolean maybeOverlapsRangeImpl(
            final long min, final long max,
            final long lower, final boolean lowerInclusive,
            final long upper, final boolean upperInclusive) {
        if (lower > upper || (lower == upper && !(lowerInclusive && upperInclusive))) {
            return false;
        }
        return (upperInclusive ? min <= upper : min < upper)
                && (lowerInclusive ? max >= lower : max > lower);
    }

    /**
     * Verifies that the statistics range intersects any point provided in the match filter.
     */
    static boolean maybeOverlaps(
            @NotNull final MatchFilter matchFilter,
            @NotNull final Statistics<?> statistics) {
        final Object[] values = matchFilter.getValues();
        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex handling
        // logic.
        // TODO (DH-19666): Improve handling of nulls.
        final long[] unboxedValues = ArrayTypeUtils.getUnboxedLongArray(values);
        for (final long value : unboxedValues) {
            if (value == QueryConstants.NULL_LONG) {
                return true;
            }
        }
        final MutableObject<Long> mutableMin = new MutableObject<>();
        final MutableObject<Long> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForLongs(statistics, mutableMin::setValue, mutableMax::setValue)) {
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
    static boolean maybeMatches(
            final long min,
            final long max,
            @NotNull final long[] values) {
        for (final long value : values) {
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
     * [Long.MIN_VALUE, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, Long.MAX_VALUE]
     * </pre>
     */
    static boolean maybeMatchesInverse(
            final long min,
            final long max,
            @NotNull final long[] values) {
        Arrays.sort(values);
        long lower = Long.MIN_VALUE;
        boolean lowerInclusive = true;
        for (final long upper : values) {
            if (maybeOverlapsRangeImpl(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlapsRangeImpl(min, max, lower, lowerInclusive, Long.MAX_VALUE, true);
    }
}
