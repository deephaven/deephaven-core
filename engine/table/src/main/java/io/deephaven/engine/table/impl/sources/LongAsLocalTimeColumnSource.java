/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@code long} to {@link LocalTime} values.
 */
public class LongAsLocalTimeColumnSource extends BoxedLongAsTimeSource<LocalTime> {
    private final ZoneId zone;

    public LongAsLocalTimeColumnSource(ColumnSource<Long> alternateColumnSource, ZoneId zone) {
        super(LocalTime.class, alternateColumnSource);
        this.zone = zone;
    }

    @Override
    protected LocalTime makeValue(long val) {
        final ZonedDateTime zdt = DateTimeUtils.epochNanosToZonedDateTime(val, zone);
        return zdt == null ? null : zdt.toLocalTime();
    }
}
