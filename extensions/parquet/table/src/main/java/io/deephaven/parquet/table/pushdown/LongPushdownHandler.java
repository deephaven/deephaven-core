//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.chunkfilter.LongChunkFilter;
import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.LongComparisons;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import static io.deephaven.parquet.table.pushdown.ParquetPushdownUtils.containsDeephavenNullLong;

@InternalUseOnly
public abstract class LongPushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final LongChunkFilter longChunkFilter,
            final MinMax<?> minMax,
            final long nullCount) {
        final long min = TypeUtils.getUnboxedLong(minMax.min());
        final long max = TypeUtils.getUnboxedLong(minMax.max());
        final boolean doStatsContainNull = nullCount > 0 || containsDeephavenNullLong(min, max);
        final boolean matchesNull = (doStatsContainNull && longChunkFilter.matches(QueryConstants.NULL_LONG));
        if (matchesNull) {
            return true;
        }
        if (filter instanceof LongRangeFilter) {
            final LongRangeFilter longRangeFilter = (LongRangeFilter) filter;
            return maybeOverlaps(
                    min, max,
                    longRangeFilter.getLower(), longRangeFilter.isLowerInclusive(),
                    longRangeFilter.getUpper(), longRangeFilter.isUpperInclusive());
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
            final long min, final long max,
            final long lower, final boolean lowerInclusive,
            final long upper, final boolean upperInclusive) {
        final int c0 = LongComparisons.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = LongComparisons.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = LongComparisons.compare(min, upper);
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
            final long min,
            final long max,
            @Nullable final Object[] values,
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
            final long min,
            final long max,
            @NotNull final Object[] values) {
        for (final Object v : values) {
            final long value = TypeUtils.getUnboxedLong(v);
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
     * [QueryConstants.NULL_LONG, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, QueryConstants.MAX_LONG]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final long min,
            final long max,
            @NotNull final Object[] values) {
        final Long[] sortedValues = sort((Long[]) values);
        long lower = QueryConstants.NULL_LONG;
        boolean lowerInclusive = true;
        for (final long upper : sortedValues) {
            if (maybeOverlaps(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlaps(min, max, lower, lowerInclusive, QueryConstants.MAX_LONG, true);
    }

    // TODO (deephaven-core#5920): Use the more efficient sorting method when available.
    private static Long[] sort(@NotNull final Long[] values) {
        // Unbox to get the primitive values, and then box them back for sorting with custom comparator.
        final Long[] boxedValues = Arrays.stream(values)
                .map(TypeUtils::getUnboxedLong)
                .toArray(Long[]::new);
        Arrays.sort(boxedValues, LongComparisons::compare);
        return boxedValues;
    }
}
