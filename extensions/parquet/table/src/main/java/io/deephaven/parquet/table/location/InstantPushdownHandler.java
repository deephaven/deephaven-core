//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

final class InstantPushdownHandler {

    static boolean maybeOverlaps(
            final InstantRangeFilter instantRangeFilter,
            final Statistics<?> statistics) {
        // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex handling
        // logic.
        // TODO (DH-19666): Improve handling of nulls
        final long dhLower = instantRangeFilter.getLower();
        final long dhUpper = instantRangeFilter.getUpper();
        if (dhLower == QueryConstants.NULL_LONG || dhUpper == QueryConstants.NULL_LONG) {
            return true;
        }
        final MutableObject<Instant> mutableMin = new MutableObject<>();
        final MutableObject<Instant> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForInstants(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so assume that we overlap.
            return true;
        }
        final long min = DateTimeUtils.epochNanos(mutableMin.getValue());
        final long max = DateTimeUtils.epochNanos(mutableMax.getValue());
        return LongPushdownHandler.maybeOverlapsRangeImpl(
                min, max,
                dhLower, instantRangeFilter.isLowerInclusive(),
                dhUpper, instantRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the statistics range intersects any point provided in the match filter.
     */
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
        final long[] instantNanos = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (!(value instanceof Instant)) {
                return true;
            }
            instantNanos[i] = DateTimeUtils.epochNanos((Instant) value);
        }

        final MutableObject<Instant> mutableMin = new MutableObject<>();
        final MutableObject<Instant> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForInstants(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so assume that we overlap.
            return true;
        }
        final long min = DateTimeUtils.epochNanos(mutableMin.getValue());
        final long max = DateTimeUtils.epochNanos(mutableMax.getValue());
        if (!invertMatch) {
            return LongPushdownHandler.maybeMatches(min, max, instantNanos);
        }
        return LongPushdownHandler.maybeMatchesInverse(min, max, instantNanos);
    }
}
