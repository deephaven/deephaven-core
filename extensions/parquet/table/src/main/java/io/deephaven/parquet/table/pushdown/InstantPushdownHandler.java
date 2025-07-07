//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;

import java.time.Instant;

@InternalUseOnly
public abstract class InstantPushdownHandler {

    public static boolean maybeOverlaps(
            final InstantRangeFilter instantRangeFilter,
            final InstantRangeFilter.InstantLongChunkFilterAdapter instantChunkFilter,
            final MinMax<?> minMax,
            final long nullCount) {
        final long min = DateTimeUtils.epochNanos((Instant) minMax.min());
        final long max = DateTimeUtils.epochNanos((Instant) minMax.max());
        final boolean matchesNull = nullCount > 0 && instantChunkFilter.matches(QueryConstants.NULL_LONG);
        if (matchesNull) {
            return true;
        }
        return LongPushdownHandler.maybeOverlaps(
                min, max,
                instantRangeFilter.getLower(), instantRangeFilter.isLowerInclusive(),
                instantRangeFilter.getUpper(), instantRangeFilter.isUpperInclusive());
    }
}
