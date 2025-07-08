//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.chunkfilter.CharChunkFilter;
import io.deephaven.engine.table.impl.select.CharRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.type.TypeUtils;

import java.util.Arrays;
import static io.deephaven.parquet.table.pushdown.ParquetPushdownUtils.containsDeephavenNullChar;

@InternalUseOnly
public abstract class CharPushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final CharChunkFilter charChunkFilter,
            final MinMax<?> minMax,
            final long nullCount) {
        final char min = TypeUtils.getUnboxedChar(minMax.min());
        final char max = TypeUtils.getUnboxedChar(minMax.max());
        final boolean doStatsContainNull = nullCount > 0 || containsDeephavenNullChar(min, max);
        final boolean matchesNull = (doStatsContainNull && charChunkFilter.matches(QueryConstants.NULL_CHAR));
        if (matchesNull) {
            return true;
        }
        if (filter instanceof CharRangeFilter) {
            final CharRangeFilter charRangeFilter = (CharRangeFilter) filter;
            return maybeOverlaps(
                    min, max,
                    charRangeFilter.getLower(), charRangeFilter.isLowerInclusive(),
                    charRangeFilter.getUpper(), charRangeFilter.isUpperInclusive());
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
            final char min, final char max,
            final char lower, final boolean lowerInclusive,
            final char upper, final boolean upperInclusive) {
        final int c0 = CharComparisons.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = CharComparisons.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = CharComparisons.compare(min, upper);
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
            final char min,
            final char max,
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
            final char min,
            final char max,
            final Object[] values) {
        for (final Object v : values) {
            final char value = TypeUtils.getUnboxedChar(v);
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
     * [QueryConstants.NULL_CHAR, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, QueryConstants.MAX_CHAR]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final char min,
            final char max,
            final Object[] values) {
        final Character[] sortedValues = sort(values);
        char lower = QueryConstants.NULL_CHAR;
        boolean lowerInclusive = true;
        for (final char upper : sortedValues) {
            if (maybeOverlaps(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlaps(min, max, lower, lowerInclusive, QueryConstants.MAX_CHAR, true);
    }

    // TODO (deephaven-core#5920): Use the more efficient sorting method when available.
    private static Character[] sort(final Object[] values) {
        // Unbox to get the primitive values, and then box them back for sorting with custom comparator.
        final Character[] boxedValues = Arrays.stream(values)
                .map(TypeUtils::getUnboxedChar)
                .toArray(Character[]::new);
        Arrays.sort(boxedValues, CharComparisons::compare);
        return boxedValues;
    }
}
