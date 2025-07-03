//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPushdownHandler and run "./gradlew replicateParquetPushdownHandlers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pushdown;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.impl.chunkfilter.FloatChunkFilter;
import io.deephaven.engine.table.impl.select.FloatRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import static io.deephaven.parquet.table.pushdown.ParquetPushdownUtils.containsDeephavenNullFloat;

@InternalUseOnly
public abstract class FloatPushdownHandler {

    public static boolean maybeOverlaps(
            final Filter filter,
            final FloatChunkFilter floatChunkFilter,
            final MinMax<?> minMax,
            final long nullCount) {
        if (floatChunkFilter.matches(QueryConstants.NAN_FLOAT)) {
            // Cannot be filtered using statistics because parquet statistics do not include NaN.
            return true;
        }
        final float min = TypeUtils.getUnboxedFloat(minMax.min());
        final float max = TypeUtils.getUnboxedFloat(minMax.max());
        final boolean doStatsContainNull = nullCount > 0 || containsDeephavenNullFloat(min, max);
        final boolean matchesNull = (doStatsContainNull && floatChunkFilter.matches(QueryConstants.NULL_FLOAT));
        if (matchesNull) {
            return true;
        }
        if (filter instanceof FloatRangeFilter) {
            final FloatRangeFilter floatRangeFilter = (FloatRangeFilter) filter;
            return maybeOverlaps(
                    min, max,
                    floatRangeFilter.getLower(), floatRangeFilter.isLowerInclusive(),
                    floatRangeFilter.getUpper(), floatRangeFilter.isUpperInclusive());
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
            final float min, final float max,
            final float lower, final boolean lowerInclusive,
            final float upper, final boolean upperInclusive) {
        final int c0 = FloatComparisons.compare(lower, upper);
        if (c0 > 0 || (c0 == 0 && !(lowerInclusive && upperInclusive))) {
            // lower > upper, no overlap possible.
            return false;
        }
        final int c1 = FloatComparisons.compare(lower, max);
        if (c1 > 0) {
            // lower > max, no overlap possible.
            return false;
        }
        final int c2 = FloatComparisons.compare(min, upper);
        if (c2 > 0) {
            // min > upper, no overlap possible.
            return false;
        }
        return (c1 < 0 && c2 < 0)
                || (c1 == 0 && lowerInclusive)
                || (c2 == 0 && upperInclusive);
    }

    /**
     * Verifies that the {@code [min, max]} range intersects every point supplied in {@code values}, taking into account
     * the {@code inverseMatch} flag.
     */
    private static boolean maybeMatches(
            final float min,
            final float max,
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
     * Verifies that the {@code [min, max]} range intersects every point supplied in {@code values}.
     */
    private static boolean maybeMatchesImpl(
            final float min,
            final float max,
            @NotNull final Object[] values) {
        for (final Object v : values) {
            final float value = TypeUtils.getUnboxedFloat(v);
            if (!maybeOverlaps(min, max, value, true, value, true)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Verifies that the {@code [min, max]} range includes values that are not in the given {@code values} array. This
     * is done by checking whether {@code [min, max]} overlaps with every open gap produced by excluding the given
     * values. For example, if the values are sorted as {@code v_0, v_1, ..., v_n-1}, then the gaps are:
     * 
     * <pre>
     * [QueryConstants.NULL_FLOAT, v_0), (v_0, v_1), ... , (v_n-2, v_n-1), (v_n-1, QueryConstants.NAN_FLOAT]
     * </pre>
     */
    private static boolean maybeMatchesInverseImpl(
            final float min,
            final float max,
            @NotNull final Object[] values) {
        final Float[] sortedValues = sort((Float[]) values);
        float lower = QueryConstants.NULL_FLOAT;
        boolean lowerInclusive = true;
        for (final float upper : sortedValues) {
            if (!maybeOverlaps(min, max, lower, lowerInclusive, upper, false)) {
                return false;
            }
            lower = upper;
            lowerInclusive = false;
        }
        return maybeOverlaps(min, max, lower, lowerInclusive, QueryConstants.NAN_FLOAT, true);
    }

    // TODO (deephaven-core#5920): Use the more efficient sorting method when available.
    private static Float[] sort(@NotNull final Float[] values) {
        // Unbox to get the primitive values, and then box them back for sorting with custom comparator.
        final Float[] boxedValues = Arrays.stream(values)
                .map(TypeUtils::getUnboxedFloat)
                .toArray(Float[]::new);
        Arrays.sort(boxedValues, FloatComparisons::compare);
        return boxedValues;
    }
}
