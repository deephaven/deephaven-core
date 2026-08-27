//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
        // Null rows are accounted for by the null guard in ParquetTableLocation.pushdownRowGroupMetadata:
        // a filter that can match null (`X < v` does) reaches this only for row groups proven to hold
        // none, and a filter that cannot match null is unaffected by their presence.
        final byte dhLower = byteRangeFilter.getLower();
        final byte dhUpper = byteRangeFilter.getUpper();
        final MutableObject<Byte> mutableMin = new MutableObject<>();
        final MutableObject<Byte> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForBytes(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
            return true;
        }
        if (dhLower == QueryConstants.NULL_BYTE) {
            // Filter is unbounded below; can only match if the upper bound is above this row group's minimum.
            return byteRangeFilter.isUpperInclusive()
                    ? mutableMin.get() <= dhUpper
                    : mutableMin.get() < dhUpper;
        }
        return maybeOverlapsRangeImpl(
                mutableMin.get(), mutableMax.get(),
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
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getMatchOptions().inverted();
        if (values == null || values.length == 0) {
            // No values to check against
            return invertMatch ? StatisticsEvaluator.ALWAYS_MAYBE : statistics -> false;
        }
        // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex handling
        // logic.
        // TODO (DH-19666): Improve handling of nulls.
        final byte[] unboxedValues = ArrayTypeUtils.getUnboxedByteArray(values);
        for (final byte value : unboxedValues) {
            if (value == QueryConstants.NULL_BYTE) {
                return StatisticsEvaluator.ALWAYS_MAYBE;
            }
        }
        if (invertMatch) {
            // Sorted once here; maybeMatchesInverse walks the gaps between adjacent values. Sorting
            // numerically is only correct because the loop above has already rejected the null sentinel:
            // for some types it does not sort where Deephaven orders it, so a null reaching this point
            // would land at the wrong end and corrupt the gap walk.
            Arrays.sort(unboxedValues);
        }
        return statistics -> {
            final MutableObject<Byte> mutableMin = new MutableObject<>();
            final MutableObject<Byte> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForBytes(statistics, mutableMin::setValue, mutableMax::setValue)) {
                // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
                return true;
            }
            return invertMatch
                    ? maybeMatchesInverse(mutableMin.get(), mutableMax.get(), unboxedValues)
                    : maybeMatches(mutableMin.get(), mutableMax.get(), unboxedValues);
        };
    }

    /**
     * Convenience for a single row group; prefer {@link #maybeCreateEvaluator} when iterating over several.
     */
    static boolean maybeOverlaps(
            @NotNull final MatchFilter matchFilter,
            @NotNull final Statistics<?> statistics) {
        return maybeCreateEvaluator(matchFilter).maybeOverlaps(statistics);
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
     * <p>
     * Gaps between adjacent values are deliberately treated as non-empty. {@code X not in (5, 6)} against statistics
     * {@code [5, 6]} reports "maybe" although no integer lies strictly between 5 and 6. Closing that would need
     * per-type successor arithmetic -- and the floating-point equivalent, where the next representable value depends on
     * the type -- for a purely performance win, in code whose failure mode is wrong results. Left as is; the tests
     * record the tighter answer in their comments.
     * 
     * where {@code ...} represents the extreme ends of the range.
     */
    static boolean maybeMatchesInverse(
            final byte min,
            final byte max,
            @NotNull final byte[] values) {
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
