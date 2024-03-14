//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@code long} to {@link LocalTime} values.
 */
public class LongAsLocalTimeColumnSource extends LongAsTimeSource<LocalTime> {
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

    @Override
    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId timeZone) {
        return zone.equals(timeZone) ? this : super.toLocalTime(timeZone);
    }
}
