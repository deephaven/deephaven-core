//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.util.annotations.InternalUseOnly;
import org.apache.parquet.column.statistics.Statistics;

import java.util.Optional;

@InternalUseOnly
public abstract class CaseInsensitiveStringMatchPushdownHandler {

    public static boolean maybeMatches(
            final MatchFilter matchFilter,
            final Statistics<?> statistics) {
        final Optional<MinMax<?>> minMaxFromStatistics = MinMaxFromStatistics.get(statistics, String.class);
        if (minMaxFromStatistics.isEmpty()) {
            // Statistics could not be processed, so we cannot determine overlaps.
            return true;
        }
        final MinMax<?> minMax = minMaxFromStatistics.get();
        final String min = (String) minMax.min();
        final String max = (String) minMax.max();
        final Object[] values = matchFilter.getValues();
        final boolean inverseMatch = matchFilter.getInvertMatch();
        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        // Skip pushdown-based filtering for nulls
        for (final Object value : values) {
            if (value == null) {
                return true;
            }
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
            final Object[] values) {
        for (final Object value : values) {
            final String valueStr = (String) value;
            if (valueStr.compareToIgnoreCase(min) >= 0 && valueStr.compareToIgnoreCase(max) <= 0) {
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
            final Object[] values) {
        // We can always insert a new value in the input range unless the range is exactly equal to the value.
        if (min.equalsIgnoreCase(max)) {
            for (final Object value : values) {
                final String valueStr = (String) value;
                if (min.compareToIgnoreCase(valueStr) == 0) {
                    return false;
                }
            }
        }
        return true;
    }
}
