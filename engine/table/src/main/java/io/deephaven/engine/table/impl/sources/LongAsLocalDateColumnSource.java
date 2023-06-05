/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@code long} to {@link LocalDate} values.
 */
public class LongAsLocalDateColumnSource extends BoxedLongAsTimeSource<LocalDate> {
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
}
