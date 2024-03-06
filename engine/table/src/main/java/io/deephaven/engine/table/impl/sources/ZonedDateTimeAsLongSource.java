//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;

import java.time.ZonedDateTime;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@link ZonedDateTime} to {@code long} values.
 */
public class ZonedDateTimeAsLongSource extends UnboxedTimeBackedColumnSource<ZonedDateTime> {

    public ZonedDateTimeAsLongSource(ColumnSource<ZonedDateTime> alternateColumnSource) {
        super(alternateColumnSource);
    }

    @Override
    protected long toEpochNano(ZonedDateTime val) {
        return DateTimeUtils.epochNanos(val);
    }
}
