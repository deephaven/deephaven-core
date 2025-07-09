//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.InternalUseOnly;
import org.apache.parquet.column.statistics.Statistics;

import java.time.Instant;
import java.util.Optional;

@InternalUseOnly
public abstract class InstantPushdownHandler {

    public static boolean maybeOverlaps(
            final InstantRangeFilter instantRangeFilter,
            final Statistics<?> statistics) {
        final Optional<MinMax<?>> minMaxFromStatistics = MinMaxFromStatistics.get(statistics, Instant.class);
        if (minMaxFromStatistics.isEmpty()) {
            // Statistics could not be processed, so we cannot determine overlaps.
            return true;
        }
        final MinMax<?> minMax = minMaxFromStatistics.get();
        final long min = DateTimeUtils.epochNanos((Instant) minMax.min());
        final long max = DateTimeUtils.epochNanos((Instant) minMax.max());
        return LongPushdownHandler.maybeOverlaps(min, max, instantRangeFilter);
    }
}
