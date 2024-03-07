//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.*;
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
        implements ColumnSourceGetDefaults.ForObject<ZonedDateTime>, ConvertibleTimeSource,
        ConvertibleTimeSource.Zoned {
    private final ZoneId zone;

    public static ColumnSource<LocalDate> asLocalDate(ZoneId zone, RegionedColumnSourceLong<Values> inner) {
        return new LongAsLocalDateColumnSource(inner, zone);
    }

    public static ColumnSource<LocalTime> asLocalTime(ZoneId zone, RegionedColumnSourceLong<Values> inner) {
        return new LongAsLocalTimeColumnSource(inner, zone);
    }

    public RegionedColumnSourceZonedDateTime(
            @NotNull final ZoneId zone,
            @NotNull final RegionedColumnSourceLong<Values> inner) {
        super(ColumnRegionLong.createNull(PARAMETERS.regionMask), ZonedDateTime.class, inner);
        this.zone = zone;
    }

    @Override
    public void convertRegion(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final Chunk<? extends Values> source,
            @NotNull final RowSequence rowSequence) {
        final WritableObjectChunk<ZonedDateTime, ? super Values> objectChunk = destination.asWritableObjectChunk();
        final LongChunk<? extends Values> longChunk = source.asLongChunk();

        final int size = objectChunk.size();
        final int length = longChunk.size();

        for (int i = 0; i < length; ++i) {
            objectChunk.set(size + i, DateTimeUtils.epochNanosToZonedDateTime(longChunk.get(i), zone));
        }
        objectChunk.setSize(size + length);
    }

    @Override
    public ZonedDateTime get(final long rowKey) {
        return rowKey == RowSequence.NULL_ROW_KEY ? null
                : DateTimeUtils.epochNanosToZonedDateTime(getNativeSource().lookupRegion(rowKey).getLong(rowKey),
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
    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {
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
    public ColumnSource<Long> toEpochNano() {
        return getNativeSource();
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }
}
