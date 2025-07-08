//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.chunkfilter.ShortChunkFilter;
import io.deephaven.engine.table.impl.select.ShortRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.ShortComparisons;
import io.deephaven.util.type.TypeUtils;

import java.util.Arrays;
import static io.deephaven.parquet.table.pushdown.ParquetPushdownUtils.containsDeephavenNullShort;

@InternalUseOnly
public abstract class ShortPushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final ShortChunkFilter shortChunkFilter,
            final MinMax<?> minMax,
            final long nullCount) {
        final short min = TypeUtils.getUnboxedShort(minMax.min());
        final short max = TypeUtils.getUnboxedShort(minMax.max());
        final boolean doStatsContainNull = nullCount > 0 || containsDeephavenNullShort(min, max);
        final boolean matchesNull = (doStatsContainNull && shortChunkFilter.matches(QueryConstants.NULL_SHORT));
        if (matchesNull) {
            return true;
        }
        if (filter instanceof ShortRangeFilter) {
            final ShortRangeFilter shortRangeFilter = (ShortRangeFilter) filter;
            return maybeOverlaps(
                    min, max,
                    shortRangeFilter.getLower(), shortRangeFilter.isLowerInclusive(),
                    shortRangeFilter.getUpper(), shortRangeFilter.isUpperInclusive());
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
            final short min, final short max,
            final short lower, final boolean lowerInclusive,
            final short upper, final boolean upperInclusive) {
        final int c0 = ShortComparisons.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = ShortComparisons.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = ShortComparisons.compare(min, upper);
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
            final short min,
            final short max,
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
            final short min,
            final short max,
            final Object[] values) {
        for (final Object v : values) {
            final short value = TypeUtils.getUnboxedShort(v);
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
     * [QueryConstants.NULL_SHORT, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, QueryConstants.MAX_SHORT]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final short min,
            final short max,
            final Object[] values) {
        final Short[] sortedValues = sort(values);
        short lower = QueryConstants.NULL_SHORT;
        boolean lowerInclusive = true;
        for (final short upper : sortedValues) {
            if (maybeOverlaps(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlaps(min, max, lower, lowerInclusive, QueryConstants.MAX_SHORT, true);
    }

    // TODO (deephaven-core#5920): Use the more efficient sorting method when available.
    private static Short[] sort(final Object[] values) {
        // Unbox to get the primitive values, and then box them back for sorting with custom comparator.
        final Short[] boxedValues = Arrays.stream(values)
                .map(TypeUtils::getUnboxedShort)
                .toArray(Short[]::new);
        Arrays.sort(boxedValues, ShortComparisons::compare);
        return boxedValues;
    }
}
