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
        final boolean inverseMatch = matchFilter.getInvertMatch();
        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        final Comparable<?>[] comparableValues = new Comparable<?>[values.length];
        for (int i = 0; i < values.length; i++) {
            if (!(values[i] instanceof String)) {
                // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex
                // handling logic.
                // TODO (DH-19666): Improve handling of nulls
                return true;
            }
            // Convert to lower case for case-insensitive comparison
            comparableValues[i] = ((String) values[i]).toLowerCase();
        }
        final MutableObject<String> mutableMin = new MutableObject<>();
        final MutableObject<String> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForStrings(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
            return true;
        }
        final String updatedMin = mutableMin.getValue().toLowerCase();
        final String updatedMax = mutableMax.getValue().toLowerCase();
        if (!inverseMatch) {
            return ComparablePushdownHandler.maybeMatches(updatedMin, updatedMax, comparableValues);
        }
        return ComparablePushdownHandler.maybeMatchesInverse(updatedMin, updatedMax, comparableValues);
    }
}
