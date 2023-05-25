/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@link DateTime} to {@code long} values.
 */
public class DateTimeAsLongColumnSource extends UnboxedTimeBackedColumnSource<DateTime> {
    public DateTimeAsLongColumnSource(ColumnSource<DateTime> alternateColumnSource) {
        super(alternateColumnSource);
    }

    @Override
    protected long toEpochNano(DateTime val) {
        return DateTimeUtils.nanos(val);
    }
}
