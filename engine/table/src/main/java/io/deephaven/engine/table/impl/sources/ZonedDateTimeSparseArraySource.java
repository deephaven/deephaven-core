/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.TimeZone;
import org.jetbrains.annotations.NotNull;

import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Array-backed ColumnSource for {@link ZonedDateTime}s. Allows reinterpretation as long.
 */
public class ZonedDateTimeSparseArraySource extends NanosBasedTimeSparseArraySource<ZonedDateTime>
        implements MutableColumnSourceGetDefaults.ForObject<ZonedDateTime>, ConvertableTimeSource.Zoned {
    private final ZoneId zone;

    public ZonedDateTimeSparseArraySource(final @NotNull String zone) {
        this(ZoneId.of(zone));
    }

    public ZonedDateTimeSparseArraySource(final @NotNull TimeZone zone) {
        this(zone.getZoneId());
    }

    public ZonedDateTimeSparseArraySource(final @NotNull ZoneId zone) {
        this(zone, new LongSparseArraySource());
    }

    public ZonedDateTimeSparseArraySource(final @NotNull ZoneId zone, final @NotNull LongSparseArraySource nanoSource) {
        super(ZonedDateTime.class, nanoSource);
        this.zone = zone;
    }

    @Override
    protected ZonedDateTime makeValue(long nanos) {
        return DateTimeUtils.makeZonedDateTime(nanos, zone);
    }

    @Override
    protected long toNanos(ZonedDateTime value) {
        return DateTimeUtils.toEpochNano(value);
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(final @NotNull ZoneId zone) {
        if (this.zone.equals(zone)) {
            return this;
        }

        return new ZonedDateTimeSparseArraySource(zone, this.nanoSource);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }
}
