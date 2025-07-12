//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.engine.table.impl.select.ComparableRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.ObjectComparisons;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

@InternalUseOnly
public abstract class ObjectPushdownHandler {

    public static boolean maybeOverlaps(
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
        return maybeOverlapsImpl(
                mutableMin.getValue(), mutableMax.getValue(),
                dhLower, comparableRangeFilter.isLowerInclusive(),
                dhUpper, comparableRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlapsImpl(
            @NotNull final Comparable<?> min, @NotNull final Comparable<?> max,
            @NotNull final Comparable<?> lower, final boolean lowerInclusive,
            @NotNull final Comparable<?> upper, final boolean upperInclusive) {
        final int c0 = ((Comparable) lower).compareTo(upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = ((Comparable) lower).compareTo(max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = ((Comparable) min).compareTo(upper);
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
    public static boolean maybeOverlaps(
            @NotNull final MatchFilter matchFilter,
            @NotNull final Statistics<?> statistics) {
        final Object[] values = matchFilter.getValues();
        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
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
        if (!matchFilter.getInvertMatch()) {
            return maybeMatchesImpl(mutableMin.getValue(), mutableMax.getValue(), comparableValues);
        }
        return maybeMatchesInverseImpl(mutableMin.getValue(), mutableMax.getValue(), comparableValues);
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    private static boolean maybeMatchesImpl(
            @NotNull final Comparable<?> min,
            @NotNull final Comparable<?> max,
            @NotNull final Comparable<?>[] values) {
        for (final Comparable<?> value : values) {
            if (maybeOverlapsImpl(min, max, value, true, value, true)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Verifies that the {@code [min, max]} range includes any value that is not in the given {@code values} array.
     */
    private static boolean maybeMatchesInverseImpl(
            @NotNull final Comparable<?> min,
            @NotNull final Comparable<?> max,
            @NotNull final Comparable<?>[] values) {
        if (ObjectComparisons.eq(min, max)) {
            for (final Comparable<?> value : values) {
                if (ObjectComparisons.eq(min, value)) {
                    // This is the only case where we can definitely say there is no match.
                    return false;
                }
            }
        }
        return true;
    }
}
