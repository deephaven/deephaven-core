//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.InternalUseOnly;

import java.time.Instant;

@InternalUseOnly
public abstract class InstantPushdownHandler {

    public static boolean maybeOverlaps(
            final InstantRangeFilter instantRangeFilter,
            final MinMax<?> minMax) {
        final long min = DateTimeUtils.epochNanos((Instant) minMax.min());
        final long max = DateTimeUtils.epochNanos((Instant) minMax.max());
        return LongPushdownHandler.maybeOverlaps(min, max, instantRangeFilter);
    }
}
