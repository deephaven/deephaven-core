//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
import java.util.Arrays;

final class InstantPushdownHandler {

    static boolean maybeOverlaps(
            final InstantRangeFilter instantRangeFilter,
            final Statistics<?> statistics) {
        // Null rows are accounted for by the null guard in ParquetTableLocation.pushdownRowGroupMetadata:
        // a filter that can match null (`X < v` does) reaches this only for row groups proven to hold
        // none, and a filter that cannot match null is unaffected by their presence.
        final long dhLower = instantRangeFilter.getLower();
        final long dhUpper = instantRangeFilter.getUpper();
        final MutableObject<Instant> mutableMin = new MutableObject<>();
        final MutableObject<Instant> mutableMax = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForInstants(statistics, mutableMin::setValue, mutableMax::setValue)) {
            // Statistics could not be processed, so assume that we overlap.
            return true;
        }
        final long min = DateTimeUtils.epochNanos(mutableMin.get());
        final long max = DateTimeUtils.epochNanos(mutableMax.get());
        if (dhLower == QueryConstants.NULL_LONG) {
            // Filter is unbounded below; can only match if the upper bound is above this row group's minimum.
            return instantRangeFilter.isUpperInclusive() ? min <= dhUpper : min < dhUpper;
        }
        return LongPushdownHandler.maybeOverlapsRangeImpl(
                min, max,
                dhLower, instantRangeFilter.isLowerInclusive(),
                dhUpper, instantRangeFilter.isUpperInclusive());
    }

    /**
     * Verifies that the statistics range intersects any point provided in the match filter.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getMatchOptions().inverted();
        if (values == null || values.length == 0) {
            // No values to check against
            return invertMatch ? StatisticsEvaluator.ALWAYS_MAYBE : statistics -> false;
        }
        // Skip pushdown-based filtering for nulls to err on the safer side instead of adding more complex handling
        // logic.
        // TODO (DH-19666): Improve handling of nulls
        final long[] instantNanos = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (!(value instanceof Instant)) {
                return StatisticsEvaluator.ALWAYS_MAYBE;
            }
            instantNanos[i] = DateTimeUtils.epochNanos((Instant) value);
        }
        if (invertMatch) {
            // LongPushdownHandler.maybeMatchesInverse walks the gaps between adjacent values and requires
            // them sorted; sorted once here rather than for every row group. Values that are not Instants
            // -- null among them -- were rejected above, so nothing can sort to the wrong end.
            Arrays.sort(instantNanos);
        }
        return statistics -> {
            final MutableObject<Instant> mutableMin = new MutableObject<>();
            final MutableObject<Instant> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForInstants(statistics, mutableMin::setValue, mutableMax::setValue)) {
                // Statistics could not be processed, so assume that we overlap.
                return true;
            }
            final long min = DateTimeUtils.epochNanos(mutableMin.get());
            final long max = DateTimeUtils.epochNanos(mutableMax.get());
            return invertMatch
                    ? LongPushdownHandler.maybeMatchesInverse(min, max, instantNanos)
                    : LongPushdownHandler.maybeMatches(min, max, instantNanos);
        };
    }

    /**
     * Convenience for a single row group; prefer {@link #maybeCreateEvaluator} when iterating over several.
     */
    static boolean maybeOverlaps(
            @NotNull final MatchFilter matchFilter,
            @NotNull final Statistics<?> statistics) {
        return maybeCreateEvaluator(matchFilter).maybeOverlaps(statistics);
    }
}
