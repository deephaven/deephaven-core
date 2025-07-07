//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.chunkfilter.DoubleChunkFilter;
import io.deephaven.engine.table.impl.select.DoubleRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import static io.deephaven.parquet.table.pushdown.ParquetPushdownUtils.containsDeephavenNullDouble;

@InternalUseOnly
public abstract class DoublePushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final DoubleChunkFilter doubleChunkFilter,
            final MinMax<?> minMax,
            final long nullCount) {
        if (doubleChunkFilter.matches(QueryConstants.NAN_DOUBLE)) {
            // Cannot be filtered using statistics because parquet statistics do not include NaN.
            return true;
        }
        final double min = TypeUtils.getUnboxedDouble(minMax.min());
        final double max = TypeUtils.getUnboxedDouble(minMax.max());
        final boolean doStatsContainNull = nullCount > 0 || containsDeephavenNullDouble(min, max);
        final boolean matchesNull = (doStatsContainNull && doubleChunkFilter.matches(QueryConstants.NULL_DOUBLE));
        if (matchesNull) {
            return true;
        }
        if (filter instanceof DoubleRangeFilter) {
            final DoubleRangeFilter doubleRangeFilter = (DoubleRangeFilter) filter;
            return maybeOverlaps(
                    min, max,
                    doubleRangeFilter.getLower(), doubleRangeFilter.isLowerInclusive(),
                    doubleRangeFilter.getUpper(), doubleRangeFilter.isUpperInclusive());
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
            final double min, final double max,
            final double lower, final boolean lowerInclusive,
            final double upper, final boolean upperInclusive) {
        final int c0 = DoubleComparisons.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = DoubleComparisons.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = DoubleComparisons.compare(min, upper);
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
            final double min,
            final double max,
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
            final double min,
            final double max,
            @NotNull final Object[] values) {
        for (final Object v : values) {
            final double value = TypeUtils.getUnboxedDouble(v);
            if (maybeOverlaps(min, max, value, true, value, true)) {
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
     * [QueryConstants.NULL_DOUBLE, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, QueryConstants.NAN_DOUBLE]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final double min,
            final double max,
            @NotNull final Object[] values) {
        final Double[] sortedValues = sort((Double[]) values);
        double lower = QueryConstants.NULL_DOUBLE;
        boolean lowerInclusive = true;
        for (final double upper : sortedValues) {
            if (maybeOverlaps(min, max, lower, lowerInclusive, upper, false)) {
                return true;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlaps(min, max, lower, lowerInclusive, QueryConstants.NAN_DOUBLE, true);
    }

    // TODO (deephaven-core#5920): Use the more efficient sorting method when available.
    private static Double[] sort(@NotNull final Double[] values) {
        // Unbox to get the primitive values, and then box them back for sorting with custom comparator.
        final Double[] boxedValues = Arrays.stream(values)
                .map(TypeUtils::getUnboxedDouble)
                .toArray(Double[]::new);
        Arrays.sort(boxedValues, DoubleComparisons::compare);
        return boxedValues;
    }
}
