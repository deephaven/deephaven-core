//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;

import java.time.Instant;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@link Instant} to {@code long} values.
 */
public class InstantAsLongColumnSource extends UnboxedTimeBackedColumnSource<Instant> {
    public InstantAsLongColumnSource(ColumnSource<Instant> alternateColumnSource) {
        super(alternateColumnSource);
    }

    @Override
    protected long toEpochNano(Instant val) {
        return DateTimeUtils.epochNanos(val);
    }
}
