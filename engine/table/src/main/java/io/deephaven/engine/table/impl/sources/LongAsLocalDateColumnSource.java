//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@code long} to {@link LocalDate} values.
 */
public class LongAsLocalDateColumnSource extends LongAsTimeSource<LocalDate> {

    private final ZoneId zone;

    public LongAsLocalDateColumnSource(ColumnSource<Long> alternateColumnSource, ZoneId zone) {
        super(LocalDate.class, alternateColumnSource);
        this.zone = zone;
    }

    @Override
    protected LocalDate makeValue(long val) {
        final ZonedDateTime zdt = DateTimeUtils.epochNanosToZonedDateTime(val, zone);
        return zdt == null ? null : zdt.toLocalDate();
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId timeZone) {
        return zone.equals(timeZone) ? this : super.toLocalDate(timeZone);
    }
}
