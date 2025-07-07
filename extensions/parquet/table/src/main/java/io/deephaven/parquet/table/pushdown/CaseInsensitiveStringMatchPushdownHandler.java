//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.engine.table.impl.chunkfilter.StringChunkMatchFilterFactory;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.annotations.InternalUseOnly;

@InternalUseOnly
public abstract class CaseInsensitiveStringMatchPushdownHandler {

    public static boolean maybeOverlaps(
            final MatchFilter matchFilter,
            final StringChunkMatchFilterFactory.CaseInsensitiveStringChunkFilter stringChunkFilter,
            final MinMax<?> minMax,
            final long nullCount) {
        final String min = (String) minMax.min();
        final String max = (String) minMax.max();
        final boolean matchesNull = (nullCount > 0 && stringChunkFilter.matches(null));
        if (matchesNull) {
            return true;
        }
        return maybeMatches(min, max, (String[]) matchFilter.getValues(), matchFilter.getInvertMatch());
    }

    /**
     * Verifies that the {@code [min, max]} range intersects every point supplied in {@code values}, taking into account
     * the {@code inverseMatch} flag.
     */
    private static boolean maybeMatches(
            final String min,
            final String max,
            final String[] values,
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
            final String min,
            final String max,
            final String[] values) {
        for (final String value : values) {
            if (value == null) {
                // Nulls are handled separately, so we skip them here.
                continue;
            }
            if (value.compareToIgnoreCase(min) >= 0 && value.compareToIgnoreCase(max) <= 0) {
                // Found a value within the range [min, max].
                return true;
            }
        }
        return false;
    }

    /**
     * Verifies that the {@code [min, max]} range includes any value that is not in the given {@code values} array.
     */
    private static boolean maybeMatchesInverseImpl(
            final String min,
            final String max,
            final String[] values) {
        // We can always insert a new value in the input range unless the range is exactly equal to the value.
        if (min.equalsIgnoreCase(max)) {
            for (final String value : values) {
                if (min.compareToIgnoreCase(value) == 0) {
                    return false;
                }
            }
        }
        return true;
    }
}
