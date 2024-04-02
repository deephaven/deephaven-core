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
import io.deephaven.engine.table.impl.sources.ConvertibleTimeSource;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Regioned column source implementation for columns of {@link Instant}s.
 */
final class RegionedColumnSourceInstant
        extends RegionedColumnSourceReferencing<Instant, Values, Long, ColumnRegionLong<Values>>
        implements ColumnSourceGetDefaults.ForObject<Instant>, ConvertibleTimeSource {

    public RegionedColumnSourceInstant() {
        this(new RegionedColumnSourceLong.AsValues());
    }

    public RegionedColumnSourceInstant(@NotNull final RegionedColumnSourceLong<Values> inner) {
        super(ColumnRegionLong.createNull(PARAMETERS.regionMask), Instant.class, inner);
    }

    @Override
    public void convertRegion(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final Chunk<? extends Values> source,
            @NotNull final RowSequence rowSequence) {
        final WritableObjectChunk<Instant, ? super Values> objectChunk = destination.asWritableObjectChunk();
        final LongChunk<? extends Values> longChunk = source.asLongChunk();

        final int size = objectChunk.size();
        final int length = longChunk.size();

        for (int i = 0; i < length; ++i) {
            objectChunk.set(size + i, DateTimeUtils.epochNanosToInstant(longChunk.get(i)));
        }
        objectChunk.setSize(size + length);
    }

    @Override
    public Instant get(final long rowKey) {
        return rowKey == RowSequence.NULL_ROW_KEY ? null
                : DateTimeUtils.epochNanosToInstant(getNativeSource().lookupRegion(rowKey).getLong(rowKey));
    }

    @Override
    public boolean supportsTimeConversion() {
        return true;
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return this;
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {
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
}
