//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.chunkfilter.ObjectChunkFilter;
import io.deephaven.engine.table.impl.select.ComparableRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.ObjectComparisons;

@InternalUseOnly
public abstract class ObjectPushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final ObjectChunkFilter<?> objectChunkFilter,
            final MinMax<?> minMax,
            final long nullCount) {
        final Comparable<?> min = minMax.min();
        final Comparable<?> max = minMax.max();
        final boolean matchesNull = nullCount > 0 && objectChunkFilter.matches(null);
        if (matchesNull) {
            return true;
        }
        if (filter instanceof ComparableRangeFilter) {
            final ComparableRangeFilter comparableRangeFilter = (ComparableRangeFilter) filter;
            return maybeOverlaps(
                    min, max,
                    comparableRangeFilter.getLower(), comparableRangeFilter.isLowerInclusive(),
                    comparableRangeFilter.getUpper(), comparableRangeFilter.isUpperInclusive());
        } else if (filter instanceof MatchFilter) {
            final MatchFilter matchFilter = (MatchFilter) filter;
            return maybeMatches(min, max, matchFilter.getValues(), matchFilter.getInvertMatch());
        }
        return true;
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean maybeOverlaps(
            final Comparable<?> min, final Comparable<?> max,
            final Comparable<?> lower, final boolean lowerInclusive,
            final Comparable<?> upper, final boolean upperInclusive) {
        final int c0 = ObjectComparisons.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = ObjectComparisons.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = ObjectComparisons.compare(min, upper);
        if (c2 > 0) {
            // min > upper, no overlap possible.
            return false;
        }
        return (c1 < 0 && c2 < 0)
                || (c1 == 0 && lowerInclusive)
                || (c2 == 0 && upperInclusive);
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}, taking into account
     * the {@code inverseMatch} flag.
     */
    private static boolean maybeMatches(
            final Comparable<?> min,
            final Comparable<?> max,
            final Object[] values,
            final boolean inverseMatch) {
        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        if (!inverseMatch) {
            return maybeMatchesImpl(min, max, values);
        }
        return maybeMatchesInverseImpl(min, max, values);
    }

    /**
     * Verifies that the {@code [min, max]} range intersects any point supplied in {@code values}.
     */
    private static boolean maybeMatchesImpl(
            final Comparable<?> min,
            final Comparable<?> max,
            final Object[] values) {
        for (final Object value : values) {
            if (!(value instanceof Comparable)) {
                // If the values are not comparable, we cannot determine overlap.
                return true;
            }
            final Comparable<?> valueComparable = (Comparable<?>) value;
            if (maybeOverlaps(min, max, valueComparable, true, valueComparable, true)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Verifies that the {@code [min, max]} range includes any value that is not in the given {@code values} array.
     */
    private static boolean maybeMatchesInverseImpl(
            final Comparable<?> min,
            final Comparable<?> max,
            final Object[] values) {
        if (ObjectComparisons.eq(min, max)) {
            for (final Object value : values) {
                if (ObjectComparisons.eq(min, value)) {
                    // This is the only case where we can definitely say there is no match.
                    return false;
                }
            }
        }
        return true;
    }
}
