//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;/*
                                                * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
                                                */

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import org.jetbrains.annotations.NotNull;

import java.time.*;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@code long} values to various Time types.
 */
public abstract class LongAsTimeSource<TIME_TYPE> extends AbstractColumnSource<TIME_TYPE>
        implements MutableColumnSourceGetDefaults.ForObject<TIME_TYPE>, ConvertibleTimeSource {

    private final ColumnSource<Long> alternateColumnSource;

    private class BoxingFillContext implements FillContext {
        final FillContext alternateFillContext;
        final WritableLongChunk<Values> innerChunk;

        private BoxingFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateFillContext = alternateColumnSource.makeFillContext(chunkCapacity, sharedContext);
            innerChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            alternateFillContext.close();
            innerChunk.close();
        }
    }

    public LongAsTimeSource(final Class<TIME_TYPE> type, ColumnSource<Long> alternateColumnSource) {
        super(type);
        this.alternateColumnSource = alternateColumnSource;
    }

    protected abstract TIME_TYPE makeValue(long val);

    @Override
    public TIME_TYPE get(long index) {
        return makeValue(alternateColumnSource.getLong(index));
    }

    @Override
    public TIME_TYPE getPrev(long index) {
        return makeValue(alternateColumnSource.getPrevLong(index));
    }

    @Override
    public boolean isImmutable() {
        return alternateColumnSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateColumnSource.allowsReinterpret(alternateDataType)
                || alternateDataType == alternateColumnSource.getType();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        // noinspection unchecked
        return alternateDataType == alternateColumnSource.getType()
                ? (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource
                : alternateColumnSource.reinterpret(alternateDataType);
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new BoxingFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        final BoxingFillContext fillContext = (BoxingFillContext) context;
        final WritableLongChunk<Values> innerChunk = fillContext.innerChunk;
        alternateColumnSource.fillChunk(fillContext.alternateFillContext, innerChunk, rowSequence);
        convertToType(destination, innerChunk);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        final BoxingFillContext fillContext = (BoxingFillContext) context;
        final WritableLongChunk<Values> innerChunk = fillContext.innerChunk;
        alternateColumnSource.fillPrevChunk(fillContext.alternateFillContext, innerChunk, rowSequence);
        convertToType(destination, innerChunk);
    }

    private void convertToType(@NotNull WritableChunk<? super Values> destination, LongChunk<Values> innerChunk) {
        final WritableObjectChunk<TIME_TYPE, ? super Values> dest = destination.asWritableObjectChunk();
        for (int ii = 0; ii < innerChunk.size(); ++ii) {
            dest.set(ii, makeValue(innerChunk.get(ii)));
        }
        dest.setSize(innerChunk.size());
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {
        return new LongAsZonedDateTimeColumnSource(alternateColumnSource, zone);
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {
        return new LongAsLocalDateColumnSource(alternateColumnSource, zone);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {
        return new LongAsLocalTimeColumnSource(alternateColumnSource, zone);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return new LongAsInstantColumnSource(alternateColumnSource);
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return alternateColumnSource;
    }

    @Override
    public boolean supportsTimeConversion() {
        return true;
    }
}
