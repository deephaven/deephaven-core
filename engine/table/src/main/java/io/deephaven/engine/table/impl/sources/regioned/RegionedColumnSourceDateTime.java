/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.ConvertibleTimeSource;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Regioned column source implementation for columns of {@link DateTime}s.
 */
final class RegionedColumnSourceDateTime
        extends RegionedColumnSourceReferencing<DateTime, Values, Long, ColumnRegionLong<Values>>
        implements ColumnSourceGetDefaults.ForObject<DateTime>, ConvertibleTimeSource {

    public RegionedColumnSourceDateTime() {
        this(new RegionedColumnSourceLong.AsValues());
    }

    public RegionedColumnSourceDateTime(@NotNull final RegionedColumnSourceLong<Values> inner) {
        super(ColumnRegionLong.createNull(PARAMETERS.regionMask), DateTime.class, inner);
    }

    @Override
    public void convertRegion(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final Chunk<? extends Values> source, RowSequence rowSequence) {
        final WritableObjectChunk<DateTime, ? super Values> objectChunk = destination.asWritableObjectChunk();
        final LongChunk<? extends Values> longChunk = source.asLongChunk();

        final int size = objectChunk.size();
        final int length = longChunk.size();

        for (int i = 0; i < length; ++i) {
            objectChunk.set(size + i, DateTimeUtils.epochNanosToDateTime(longChunk.get(i)));
        }
        objectChunk.setSize(size + length);
    }

    @Override
    public DateTime get(final long rowKey) {
        return rowKey == RowSequence.NULL_ROW_KEY ? null
                : DateTimeUtils.epochNanosToDateTime(getNativeSource().lookupRegion(rowKey).getLong(rowKey));
    }

    @Override
    public boolean supportsTimeConversion() {
        return true;
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return new RegionedColumnSourceInstant((RegionedColumnSourceLong<Values>) getNativeSource());
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {
        return new RegionedColumnSourceZonedDateTime(zone, (RegionedColumnSourceLong<Values>) getNativeSource());
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {
        return RegionedColumnSourceZonedDateTime.asLocalDate(zone,
                (RegionedColumnSourceLong<Values>) getNativeSource());
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {
        return RegionedColumnSourceZonedDateTime.asLocalTime(zone,
                (RegionedColumnSourceLong<Values>) getNativeSource());
    }

    @Override
    public ColumnSource<DateTime> toDateTime() {
        return this;
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return getNativeSource();
    }
}
