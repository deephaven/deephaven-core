//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.ComparableRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.compare.ObjectComparisons;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

final class ComparablePushdownHandler {

    static boolean maybeOverlaps(
            @NotNull final ComparableRangeFilter comparableRangeFilter,
            @NotNull final Statistics<?> statistics) {
        // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex handling
        // logic.
        // TODO (DH-19666): Improve handling of nulls
        final Comparable<?> dhLower = comparableRangeFilter.getLower();
        final Comparable<?> dhUpper = comparableRangeFilter.getUpper();
        if (dhLower == null || dhUpper == null) {
            return true;
        }
        // Get the column type from the filter
        final Class<?> dhColumnType = comparableRangeFilter.getColumnType();
        if (dhColumnType == null) {
            throw new IllegalStateException("Filter not initialized with a column type: " + comparableRangeFilter);
        }
        final MutableObject<Comparable<?>> mutableMin = new MutableObject<>();
        final MutableObject<Comparable<?>> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForComparable(statistics, mutableMin::setValue, mutableMax::setValue,
                dhColumnType)) {
            // Statistics could not be processed, so we assume that we overlap.
            return true;
        }
        return maybeOverlapsRangeImpl(
                mutableMin.getValue(), mutableMax.getValue(),
                dhLower, comparableRangeFilter.isLowerInclusive(),
                dhUpper, comparableRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsRangeImpl(
            @NotNull final Comparable<?> min, @NotNull final Comparable<?> max,
            @NotNull final Comparable<?> lower, final boolean lowerInclusive,
            @NotNull final Comparable<?> upper, final boolean upperInclusive) {
        if ((upperInclusive && lowerInclusive)
                ? ObjectComparisons.gt(lower, upper)
                : ObjectComparisons.geq(lower, upper)) {
            return false; // Empty range, no overlap
        }
        // Following logic assumes (min, max) to be a continuous range and not granular. So (a,b) will be considered
        // as "maybe overlapping" with [a, b] where b follows immediately after a.
        return (upperInclusive ? ObjectComparisons.leq(min, upper) : ObjectComparisons.lt(min, upper))
                && (lowerInclusive ? ObjectComparisons.geq(max, lower) : ObjectComparisons.gt(max, lower));
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in the filter.
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
        final Comparable<?>[] comparableValues = new Comparable[values.length];
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (!(value instanceof Comparable)) {
                // Skip pushdown-based filtering for nulls or non-comparable values to err on the safer side instead of
                // adding more complex handling logic.
                // TODO (DH-19666): Improve handling of nulls
                return true;
            }
            comparableValues[i] = (Comparable<?>) value;
        }
        // Get the column type from the filter
        final Class<?> dhColumnType = matchFilter.getColumnType();
        if (dhColumnType == null) {
            throw new IllegalStateException("Filter not initialized with a column type: " + matchFilter);
        }
        final MutableObject<Comparable<?>> mutableMin = new MutableObject<>();
        final MutableObject<Comparable<?>> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForComparable(statistics, mutableMin::setValue, mutableMax::setValue,
                dhColumnType)) {
            // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
            return true;
        }
        if (!invertMatch) {
            return maybeMatches(mutableMin.getValue(), mutableMax.getValue(), comparableValues);
        }
        return maybeMatchesInverse(mutableMin.getValue(), mutableMax.getValue(), comparableValues);
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    static boolean maybeMatches(
            @NotNull final Comparable<?> min,
            @NotNull final Comparable<?> max,
            @NotNull final Comparable<?>[] values) {
        for (final Comparable<?> value : values) {
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
            @NotNull final Comparable<?> min,
            @NotNull final Comparable<?> max,
            @NotNull final Comparable<?>[] values) {
        Arrays.sort(values);
        if (ObjectComparisons.lt(min, values[0])) {
            return true;
        }
        final int numValues = values.length;
        for (int i = 0; i < numValues - 1; i++) {
            if (maybeOverlapsRangeImpl(min, max, values[i], false, values[i + 1], false)) {
                return true;
            }
        }
        if (ObjectComparisons.gt(max, values[values.length - 1])) {
            return true;
        }
        return false;
    }
}
