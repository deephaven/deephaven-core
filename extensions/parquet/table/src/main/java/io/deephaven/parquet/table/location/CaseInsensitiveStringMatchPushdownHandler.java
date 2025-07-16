//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.MatchFilter;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

final class CaseInsensitiveStringMatchPushdownHandler {

    static boolean maybeOverlaps(
            @NotNull final MatchFilter matchFilter,
            @NotNull final Statistics<?> statistics) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getInvertMatch();
        if (values == null || values.length == 0) {
            // No values to check against
            return invertMatch;
        }
        // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex handling
        // logic.
        // TODO (DH-19666): Improve handling of nulls
        for (final Object value : values) {
            if (value == null) {
                return true;
            }
        }
        final MutableObject<String> mutableMin = new MutableObject<>();
        final MutableObject<String> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForStrings(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
            return true;
        }
        if (!invertMatch) {
            return maybeMatchesImpl(mutableMin.getValue(), mutableMax.getValue(), values);
        }
        return maybeMatchesInverseImpl(mutableMin.getValue(), mutableMax.getValue(), values);
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
                if (min.equalsIgnoreCase(valueStr)) {
                    return false;
                }
            }
        }
        return true;
    }
}
