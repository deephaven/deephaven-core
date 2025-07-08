//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.chunkfilter.IntChunkFilter;
import io.deephaven.engine.table.impl.select.IntRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.IntComparisons;
import io.deephaven.util.type.TypeUtils;

import java.util.Arrays;
import static io.deephaven.parquet.table.pushdown.ParquetPushdownUtils.containsDeephavenNullInt;

@InternalUseOnly
public abstract class IntPushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final IntChunkFilter intChunkFilter,
            final MinMax<?> minMax,
            final long nullCount) {
        final int min = TypeUtils.getUnboxedInt(minMax.min());
        final int max = TypeUtils.getUnboxedInt(minMax.max());
        final boolean doStatsContainNull = nullCount > 0 || containsDeephavenNullInt(min, max);
        final boolean matchesNull = (doStatsContainNull && intChunkFilter.matches(QueryConstants.NULL_INT));
        if (matchesNull) {
            return true;
        }
        if (filter instanceof IntRangeFilter) {
            final IntRangeFilter intRangeFilter = (IntRangeFilter) filter;
            return maybeOverlaps(
                    min, max,
                    intRangeFilter.getLower(), intRangeFilter.isLowerInclusive(),
                    intRangeFilter.getUpper(), intRangeFilter.isUpperInclusive());
        } else if (filter instanceof MatchFilter) {
            final MatchFilter matchFilter = (MatchFilter) filter;
            return maybeMatches(min, max, matchFilter.getValues(), matchFilter.getInvertMatch());
        }
        return true;
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    static boolean maybeOverlaps(
            final int min, final int max,
            final int lower, final boolean lowerInclusive,
            final int upper, final boolean upperInclusive) {
        final int c0 = IntComparisons.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = IntComparisons.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = IntComparisons.compare(min, upper);
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
            final int min,
            final int max,
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
            final int min,
            final int max,
            final Object[] values) {
        for (final Object v : values) {
            final int value = TypeUtils.getUnboxedInt(v);
            if (maybeOverlaps(min, max, value, true, value, true)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Verifies that the {@code [min, max]} range includes any value that is not in the given {@code values} array. This
     * is done by checking whether {@code [min, max]} overlaps with any open gap produced by excluding the given values.
     * For example, if the values are sorted as {@code v_0, v_1, ..., v_n-1}, then the gaps are:
     * 
     * <pre>
     * [QueryConstants.NULL_INT, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, QueryConstants.MAX_INT]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final int min,
            final int max,
            final Object[] values) {
        final Integer[] sortedValues = sort(values);
        int lower = QueryConstants.NULL_INT;
        boolean lowerInclusive = true;
        for (final int upper : sortedValues) {
            if (maybeOverlaps(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlaps(min, max, lower, lowerInclusive, QueryConstants.MAX_INT, true);
    }

    // TODO (deephaven-core#5920): Use the more efficient sorting method when available.
    private static Integer[] sort(final Object[] values) {
        // Unbox to get the primitive values, and then box them back for sorting with custom comparator.
        final Integer[] boxedValues = Arrays.stream(values)
                .map(TypeUtils::getUnboxedInt)
                .toArray(Integer[]::new);
        Arrays.sort(boxedValues, IntComparisons::compare);
        return boxedValues;
    }
}
