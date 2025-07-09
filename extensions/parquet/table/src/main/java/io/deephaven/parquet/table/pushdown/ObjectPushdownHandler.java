//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.select.ComparableRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.ObjectComparisons;
import org.jetbrains.annotations.NotNull;

@InternalUseOnly
public abstract class ObjectPushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final MinMax<?> minMax) {
        final Comparable<?> min = minMax.min();
        final Comparable<?> max = minMax.max();
        if (filter instanceof ComparableRangeFilter) {
            return maybeOverlaps(min, max, (ComparableRangeFilter) filter);
        } else if (filter instanceof MatchFilter) {
            return maybeMatches(min, max, (MatchFilter) filter);
        }
        return true;
    }

    private static boolean maybeOverlaps(
            final Comparable<?> min,
            final Comparable<?> max,
            final ComparableRangeFilter comparableRangeFilter) {
        // Skip pushdown-based filtering for nulls.
        final Comparable<?> dhLower = comparableRangeFilter.getLower();
        final Comparable<?> dhUpper = comparableRangeFilter.getUpper();
        if (dhLower == null || dhUpper == null) {
            return true;
        }
        return maybeOverlapsImpl(
                min, max,
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
    private static boolean maybeMatches(
            @NotNull final Comparable<?> min,
            @NotNull final Comparable<?> max,
            final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getInvertMatch();
        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        final Comparable<?>[] comparableValues = new Comparable[values.length];
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (value == null || !(value instanceof Comparable)) {
                // Skip pushdown-based filtering for nulls or non-comparable values.
                return true;
            }
            comparableValues[i] = (Comparable<?>) value;
        }

        if (!invertMatch) {
            return maybeMatchesImpl(min, max, comparableValues);
        }
        return maybeMatchesInverseImpl(min, max, comparableValues);
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
