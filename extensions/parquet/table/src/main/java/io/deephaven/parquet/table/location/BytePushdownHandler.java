//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.ByteRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

final class BytePushdownHandler {

    /**
     * Verifies that the statistics range intersects the range defined by the filter.
     */
    static boolean maybeOverlaps(
            @NotNull final ByteRangeFilter byteRangeFilter,
            @NotNull final Statistics<?> statistics) {
        // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex handling
        // logic.
        // TODO (DH-19666): Improve handling of nulls
        final byte dhLower = byteRangeFilter.getLower();
        final byte dhUpper = byteRangeFilter.getUpper();
        if (dhLower == QueryConstants.NULL_BYTE || dhUpper == QueryConstants.NULL_BYTE) {
            return true;
        }
        final MutableObject<Byte> mutableMin = new MutableObject<>();
        final MutableObject<Byte> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForBytes(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
            return true;
        }
        return maybeOverlapsRangeImpl(
                mutableMin.getValue(), mutableMax.getValue(),
                dhLower, byteRangeFilter.isLowerInclusive(),
                dhUpper, byteRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    static boolean maybeOverlapsRangeImpl(
            final byte min, final byte max,
            final byte lower, final boolean lowerInclusive,
            final byte upper, final boolean upperInclusive) {
        if ((upperInclusive && lowerInclusive) ? lower > upper : lower >= upper) {
            return false; // Empty range, no overlap
        }
        // Following logic assumes (min, max) to be a continuous range and not granular. So (a,b) will be considered
        // as "maybe overlapping" with [a, b] where b follows immediately after a.
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
        final boolean invertMatch = matchFilter.getInvertMatch();
        if (values == null || values.length == 0) {
            // No values to check against
            return invertMatch;
        }
        // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex handling
        // logic.
        // TODO (DH-19666): Improve handling of nulls.
        final byte[] unboxedValues = ArrayTypeUtils.getUnboxedByteArray(values);
        for (final byte value : unboxedValues) {
            if (value == QueryConstants.NULL_BYTE) {
                return true;
            }
        }
        final MutableObject<Byte> mutableMin = new MutableObject<>();
        final MutableObject<Byte> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForBytes(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
            return true;
        }
        if (!invertMatch) {
            return maybeMatches(mutableMin.getValue(), mutableMax.getValue(), unboxedValues);
        }
        return maybeMatchesInverse(mutableMin.getValue(), mutableMax.getValue(), unboxedValues);
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    static boolean maybeMatches(
            final byte min,
            final byte max,
            @NotNull final byte[] values) {
        for (final byte value : values) {
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
     * [..., v_0), (v_0, v_1), . . , (v_n-2, v_n-1), (v_n-1, ...]
     * </pre>
     * 
     * where {@code ...} represents the extreme ends of the range.
     */
    static boolean maybeMatchesInverse(
            final byte min,
            final byte max,
            @NotNull final byte[] values) {
        Arrays.sort(values);
        if (min < values[0]) {
            return true;
        }
        final int numValues = values.length;
        for (int i = 0; i < numValues - 1; i++) {
            if (maybeOverlapsRangeImpl(min, max, values[i], false, values[i + 1], false)) {
                return true;
            }
        }
        if (max > values[numValues - 1]) {
            return true;
        }
        return false;
    }
}
