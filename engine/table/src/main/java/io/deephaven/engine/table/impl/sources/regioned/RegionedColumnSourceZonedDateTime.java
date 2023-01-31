package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ConvertableTimeSource;
import io.deephaven.engine.table.impl.sources.LocalDateWrapperSource;
import io.deephaven.engine.table.impl.sources.LocalTimeWrapperSource;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Regioned column source implementation for columns of {@link ZonedDateTime}s.
 */
final class RegionedColumnSourceZonedDateTime
        extends RegionedColumnSourceReferencing<ZonedDateTime, Values, Long, ColumnRegionLong<Values>>
        implements ColumnSourceGetDefaults.ForObject<ZonedDateTime>, ConvertableTimeSource,
        ConvertableTimeSource.Zoned {
    private final ZoneId zone;

    public static ColumnSource<LocalDate> asLocalDate(ZoneId zone, RegionedColumnSourceLong<Values> inner) {
        return new LocalDateWrapperSource(new RegionedColumnSourceZonedDateTime(zone, inner), zone);
    }

    public static ColumnSource<LocalTime> asLocalTime(ZoneId zone, RegionedColumnSourceLong<Values> inner) {
        return new LocalTimeWrapperSource(new RegionedColumnSourceZonedDateTime(zone, inner), zone);
    }

    public RegionedColumnSourceZonedDateTime(final @NotNull ZoneId zone,
            final @NotNull RegionedColumnSourceLong<Values> inner) {
        super(ColumnRegionLong.createNull(PARAMETERS.regionMask), ZonedDateTime.class, inner);
        this.zone = zone;
    }

    @Override
    public void convertRegion(WritableChunk<? super Values> destination,
            Chunk<? extends Values> source, RowSequence rowSequence) {
        WritableObjectChunk<ZonedDateTime, ? super Values> objectChunk = destination.asWritableObjectChunk();
        LongChunk<? extends Values> longChunk = source.asLongChunk();

        final int size = objectChunk.size();
        final int length = longChunk.size();

        for (int i = 0; i < length; ++i) {
            objectChunk.set(size + i, DateTimeUtils.makeZonedDateTime(longChunk.get(i), zone));
        }
        objectChunk.setSize(size + length);
    }

    @Override
    public ZonedDateTime get(long elementIndex) {
        return elementIndex == RowSequence.NULL_ROW_KEY ? null
                : DateTimeUtils.makeZonedDateTime(getNativeSource().lookupRegion(elementIndex).getLong(elementIndex),
                        zone);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return new RegionedColumnSourceInstant((RegionedColumnSourceLong<Values>) getNativeSource());
    }

    @Override
    public boolean supportsTimeConversion() {
        return true;
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(final @NotNull ZoneId zone) {
        if (this.zone.equals(zone)) {
            return this;
        }
        return new RegionedColumnSourceZonedDateTime(zone, (RegionedColumnSourceLong<Values>) getNativeSource());
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(ZoneId zone) {
        return RegionedColumnSourceZonedDateTime.asLocalDate(zone,
                (RegionedColumnSourceLong<Values>) getNativeSource());
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(ZoneId zone) {
        return RegionedColumnSourceZonedDateTime.asLocalTime(zone,
                (RegionedColumnSourceLong<Values>) getNativeSource());
    }

    @Override
    public ColumnSource<DateTime> toDateTime() {
        return new RegionedColumnSourceDateTime((RegionedColumnSourceLong<Values>) getNativeSource());
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return getNativeSource();
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }
}
