//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
public class ZonedDateTimeSparseArraySource extends NanosBasedTimeSparseArraySource<ZonedDateTime>
        implements MutableColumnSourceGetDefaults.ForObject<ZonedDateTime>, ConvertibleTimeSource.Zoned {
    private final ZoneId zone;

    public ZonedDateTimeSparseArraySource(@NotNull final String zone) {
        this(ZoneId.of(zone));
    }

    public ZonedDateTimeSparseArraySource(@NotNull final ZoneId zone) {
        this(zone, new LongSparseArraySource());
    }

    public ZonedDateTimeSparseArraySource(@NotNull final ZoneId zone, @NotNull final LongSparseArraySource nanoSource) {
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

        return new ZonedDateTimeSparseArraySource(zone, this.nanoSource);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }
}
