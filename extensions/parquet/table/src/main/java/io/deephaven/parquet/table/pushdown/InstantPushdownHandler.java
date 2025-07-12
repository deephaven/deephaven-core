//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

import static io.deephaven.parquet.table.pushdown.LongPushdownHandler.maybeMatches;
import static io.deephaven.parquet.table.pushdown.LongPushdownHandler.maybeMatchesInverse;

@InternalUseOnly
public abstract class InstantPushdownHandler {

    public static boolean maybeOverlaps(
            final InstantRangeFilter instantRangeFilter,
            final Statistics<?> statistics) {
        // Skip pushdown-based filtering for nulls
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
    public static boolean maybeOverlaps(
            @NotNull final MatchFilter matchFilter,
            @NotNull final Statistics<?> statistics) {
        final Object[] values = matchFilter.getValues();
        if (values == null || values.length == 0) {
            // No values to check against, so we consider it as a maybe overlap.
            return true;
        }
        // Skip pushdown-based filtering for nulls
        final long[] instantNanos = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (!(value instanceof Instant)) {
                // Skip pushdown-based filtering for nulls or non-comparable values.
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
        if (!matchFilter.getInvertMatch()) {
            return maybeMatches(min, max, instantNanos);
        }
        return maybeMatchesInverse(min, max, instantNanos);
    }
}
