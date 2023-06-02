/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ConvertibleTimeSource;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * ImmutableArraySource for {@link ZonedDateTime}s. Allows reinterpretation as long.
 */
public class ImmutableZonedDateTimeArraySource extends ImmutableNanosBasedTimeArraySource<ZonedDateTime>
        implements ImmutableColumnSourceGetDefaults.ForObject<ZonedDateTime>, ConvertibleTimeSource.Zoned {
    private final ZoneId zone;

    public ImmutableZonedDateTimeArraySource(
            final @NotNull ZoneId zone) {
        super(ZonedDateTime.class);
        this.zone = zone;
    }

    public ImmutableZonedDateTimeArraySource(
            final @NotNull ZoneId zone,
            final @NotNull ImmutableLongArraySource nanoSource) {
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
    public ColumnSource<ZonedDateTime> toZonedDateTime(final @NotNull ZoneId zone) {
        if (this.zone.equals(zone)) {
            return this;
        }

        return new ImmutableZonedDateTimeArraySource(zone, this.nanoSource);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }
}
