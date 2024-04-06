//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * A {@link ColumnSource} for {@link LocalDate}s that is backed by an inner {@link ColumnSource} of
 * {@link ZonedDateTime}. Note that no special handling is done for timezones. The returned local time will be in the
 * zone of each inner ZonedDateTime.
 */
public class LocalDateWrapperSource extends AbstractColumnSource<LocalDate>
        implements MutableColumnSourceGetDefaults.ForObject<LocalDate> {
    private final ColumnSource<ZonedDateTime> inner;
    private final ZoneId zone;
    private final boolean mustInspectZone;

    private class ConvertingFillContext implements ChunkSource.FillContext {
        final ChunkSource.FillContext alternateFillContext;
        final WritableObjectChunk<ZonedDateTime, Values> innerChunk;

        private ConvertingFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateFillContext = inner.makeFillContext(chunkCapacity, sharedContext);
            innerChunk = WritableObjectChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            alternateFillContext.close();
            innerChunk.close();
        }
    }

    public LocalDateWrapperSource(ColumnSource<ZonedDateTime> inner, ZoneId zone) {
        super(LocalDate.class);
        this.inner = inner;
        this.zone = zone;
        mustInspectZone = !(inner instanceof ConvertibleTimeSource.Zoned)
                || ((ConvertibleTimeSource.Zoned) inner).getZone().equals(zone);
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == ZonedDateTime.class || inner.allowsReinterpret(alternateDataType);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        // noinspection unchecked
        return alternateDataType == ZonedDateTime.class
                ? (ColumnSource<ALTERNATE_DATA_TYPE>) inner
                : inner.reinterpret(alternateDataType);
    }

    @Nullable
    @Override
    public LocalDate get(long rowKey) {
        final ZonedDateTime innerVal = adjustZone(inner.get(rowKey));
        return innerVal == null ? null : innerVal.toLocalDate();
    }

    @Nullable
    @Override
    public LocalDate getPrev(long rowKey) {
        final ZonedDateTime innerVal = adjustZone(inner.getPrev(rowKey));
        return innerVal == null ? null : innerVal.toLocalDate();
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return new ConvertingFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull ColumnSource.FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull RowSequence rowSequence) {
        final ConvertingFillContext fillContext = (ConvertingFillContext) context;
        inner.fillChunk(fillContext.alternateFillContext, fillContext.innerChunk, rowSequence);
        convertInnerChunk(dest, fillContext);
    }

    @Override
    public void fillPrevChunk(@NotNull ColumnSource.FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull RowSequence rowSequence) {
        final ConvertingFillContext fillContext = (ConvertingFillContext) context;
        inner.fillPrevChunk(fillContext.alternateFillContext, fillContext.innerChunk, rowSequence);
        convertInnerChunk(dest, fillContext);
    }

    private void convertInnerChunk(@NotNull WritableChunk<? super Values> dest, ConvertingFillContext fillContext) {
        final WritableObjectChunk<LocalDate, ? super Values> typedDest = dest.asWritableObjectChunk();
        for (int ii = 0; ii < fillContext.innerChunk.size(); ii++) {
            final ZonedDateTime zdt = adjustZone(fillContext.innerChunk.get(ii));
            typedDest.set(ii, zdt == null ? null : zdt.toLocalDate());
        }
        typedDest.setSize(fillContext.innerChunk.size());
    }

    private ZonedDateTime adjustZone(final ZonedDateTime input) {
        if (!mustInspectZone || input == null || input.getZone().equals(zone)) {
            return input;
        }

        return input.withZoneSameInstant(zone);
    }
}
