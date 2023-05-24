/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@code long} to {@link ZonedDateTime} values.
 */
public class LongAsZonedDateTimeColumnSource extends BoxedLongAsTimeSource<ZonedDateTime>
        implements ConvertibleTimeSource.Zoned {
    private final ZoneId zone;

    public LongAsZonedDateTimeColumnSource(ColumnSource<Long> alternateColumnSource, ZoneId zone) {
        super(ZonedDateTime.class, alternateColumnSource);
        this.zone = zone;
    }

    @Override
    protected ZonedDateTime makeValue(long val) {
        return DateTimeUtils.epochNanosToZonedDateTime(val, zone);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }
}
