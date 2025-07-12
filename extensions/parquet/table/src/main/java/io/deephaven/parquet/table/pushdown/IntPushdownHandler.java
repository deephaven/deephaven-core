//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.engine.table.impl.select.IntRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

@InternalUseOnly
public abstract class IntPushdownHandler {

    /**
     * Verifies that the statistics range intersects the range defined by the filter.
     */
    public static boolean maybeOverlaps(
            @NotNull final IntRangeFilter intRangeFilter,
            @NotNull final Statistics<?> statistics) {
        // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex handling logic.
        // TODO (DH-19666): Improve handling of nulls
        final int dhLower = intRangeFilter.getLower();
        final int dhUpper = intRangeFilter.getUpper();
        if (dhLower == QueryConstants.NULL_INT || dhUpper == QueryConstants.NULL_INT) {
            return true;
        }
        final MutableObject<Integer> mutableMin = new MutableObject<>();
        final MutableObject<Integer> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForInts(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
            return true;
        }
        return maybeOverlapsRangeImpl(
                mutableMin.getValue(), mutableMax.getValue(),
                dhLower, intRangeFilter.isLowerInclusive(),
                dhUpper, intRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsRangeImpl(
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
        // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex handling logic.
        // TODO (DH-19666): Improve handling of nulls
        final int[] unboxedValues = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            final int value = TypeUtils.getUnboxedInt(values[i]);
            if (value == QueryConstants.NULL_INT) {
                return true;
            }
            unboxedValues[i] = value;
        }
        final MutableObject<Integer> mutableMin = new MutableObject<>();
        final MutableObject<Integer> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForInts(statistics, mutableMin::setValue, mutableMax::setValue)) {
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
            final int min,
            final int max,
            @NotNull final int[] values) {
        for (final int value : values) {
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
     * [Integer.MIN_VALUE, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, Integer.MAX_VALUE]
     * </pre>
     */
    private static boolean maybeMatchesInverse(
            final int min,
            final int max,
            @NotNull final int[] values) {
        Arrays.sort(values);
        int lower = Integer.MIN_VALUE;
        boolean lowerInclusive = true;
        for (final int upper : values) {
            if (maybeOverlapsRangeImpl(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlapsRangeImpl(min, max, lower, lowerInclusive, Integer.MAX_VALUE, true);
    }
}
