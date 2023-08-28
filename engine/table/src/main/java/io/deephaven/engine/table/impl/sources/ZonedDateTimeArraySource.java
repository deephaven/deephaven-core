/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Array-backed ColumnSource for {@link ZonedDateTime}s. Allows reinterpretation as long.
 */
public class ZonedDateTimeArraySource extends NanosBasedTimeArraySource<ZonedDateTime>
        implements MutableColumnSourceGetDefaults.ForObject<ZonedDateTime>, ConvertibleTimeSource.Zoned {
    private final ZoneId zone;

    public ZonedDateTimeArraySource(@NotNull final String zone) {
        this(ZoneId.of(zone));
    }

    public ZonedDateTimeArraySource(@NotNull final ZoneId zone) {
        this(zone, new LongArraySource());
    }

    public ZonedDateTimeArraySource(@NotNull final ZoneId zone, @NotNull final LongArraySource nanoSource) {
        super(ZonedDateTime.class, nanoSource);
        this.zone = zone;
    }

    @Override
    protected ZonedDateTime makeValue(long nanos) {
        return DateTimeUtils.epochNanosToZonedDateTime(nanos, zone);
    }

    @Override
    protected long toNanos(ZonedDateTime value) {
        return DateTimeUtils.epochNanos(value);
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {
        if (this.zone.equals(zone)) {
            return this;
        }

        return new ZonedDateTimeArraySource(zone, this.nanoSource);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }
}
