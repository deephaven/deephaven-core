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
import io.deephaven.time.DateTime;
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
        implements ColumnSourceGetDefaults.ForObject<Instant>, ConvertableTimeSource {

    public RegionedColumnSourceInstant() {
        this(new RegionedColumnSourceLong.AsValues());
    }

    public RegionedColumnSourceInstant(final @NotNull RegionedColumnSourceLong<Values> inner) {
        super(ColumnRegionLong.createNull(PARAMETERS.regionMask), Instant.class, inner);
    }

    @Override
    public void convertRegion(
            WritableChunk<? super Values> destination,
            Chunk<? extends Values> source,
            RowSequence rowSequence) {
        WritableObjectChunk<Instant, ? super Values> objectChunk = destination.asWritableObjectChunk();
        LongChunk<? extends Values> longChunk = source.asLongChunk();

        final int size = objectChunk.size();
        final int length = longChunk.size();

        for (int i = 0; i < length; ++i) {
            objectChunk.set(size + i, DateTimeUtils.makeInstant(longChunk.get(i)));
        }
        objectChunk.setSize(size + length);
    }

    @Override
    public Instant get(long elementIndex) {
        return elementIndex == RowSequence.NULL_ROW_KEY ? null
                : DateTimeUtils.makeInstant(getNativeSource().lookupRegion(elementIndex).getLong(elementIndex));
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
    public ColumnSource<ZonedDateTime> toZonedDateTime(final @NotNull ZoneId zone) {
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
}
