//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.rowset.RowSetShiftCallback;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Array-backed ColumnSource for TIME_TYPEs. Allows reinterpret as long.
 */
public abstract class NanosBasedTimeSparseArraySource<TIME_TYPE> extends AbstractColumnSource<TIME_TYPE>
        implements FillUnordered<Values>, WritableColumnSource<TIME_TYPE>, InMemoryColumnSource,
        PossiblyImmutableColumnSource, WritableSourceWithPrepareForParallelPopulation, RowSetShiftCallback,
        ConvertibleTimeSource {

    protected final LongSparseArraySource nanoSource;

    public NanosBasedTimeSparseArraySource(@NotNull final Class<TIME_TYPE> type) {
        this(type, new LongSparseArraySource());
    }

    public NanosBasedTimeSparseArraySource(@NotNull final Class<TIME_TYPE> type,
            @NotNull final LongSparseArraySource nanoSource) {
        super(type);
        this.nanoSource = nanoSource;
    }

    @Override
    public void ensureCapacity(final long capacity, final boolean nullFilled) {
        nanoSource.ensureCapacity(capacity, nullFilled);
    }

    @Override
    public void prepareForParallelPopulation(final RowSequence rowSequence) {
        nanoSource.prepareForParallelPopulation(rowSequence);
    }

    @Override
    public int parallelPopulationAlignment() {
        return nanoSource.parallelPopulationAlignment();
    }

    @Override
    public void prepareForParallelPopulation(final RowSequence rowSequence, final boolean alignedDivisions) {
        nanoSource.prepareForParallelPopulation(rowSequence, alignedDivisions);
    }

    @Override
    public void prepareForParallelPopulationInDivisions(final RowSequence rowSequence) {
        nanoSource.prepareForParallelPopulationInDivisions(rowSequence);
    }

    @Override
    public void prepareDivisionForParallelPopulation(final RowSequence division) {
        nanoSource.prepareDivisionForParallelPopulation(division);
    }

    // region Getters & Setters
    protected abstract TIME_TYPE makeValue(final long nanos);

    protected abstract long toNanos(final TIME_TYPE value);

    @Override
    public void set(long key, TIME_TYPE value) {
        nanoSource.set(key, toNanos(value));
    }

    @Override
    public void set(long key, long value) {
        nanoSource.set(key, value);
    }

    @Override
    public void setNull(long key) {
        nanoSource.setNull(key);
    }

    @Override
    public TIME_TYPE get(long rowKey) {
        return makeValue(getLong(rowKey));
    }

    @Override
    public TIME_TYPE getPrev(long rowKey) {
        return makeValue(getPrevLong(rowKey));
    }

    @Override
    public long getLong(long rowKey) {
        return nanoSource.getLong(rowKey);
    }

    @Override
    public long getPrevLong(long rowKey) {
        return nanoSource.getPrevLong(rowKey);
    }

    @Override
    public void shift(final RowSet keysToShift, long shiftDelta) {
        nanoSource.shift(keysToShift, shiftDelta);
    }
    // endregion

    // region SparseArraySource impl
    @Override
    public void startTrackingPrevValues() {
        nanoSource.startTrackingPrevValues();
    }

    @Override
    public void setImmutable() {
        nanoSource.setImmutable();
    }

    @Override
    public boolean isImmutable() {
        return nanoSource.isImmutable();
    }

    // endregion

    // region Chunking
    private class ToTimeFillContext implements FillContext {
        private final GetContext nanoGetContext;
        private final FillContext nanoFillContext;
        private final WritableLongChunk<Values> nanoChunk;

        private ToTimeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            nanoGetContext = nanoSource.makeGetContext(chunkCapacity, sharedContext);
            nanoFillContext = nanoSource.makeFillContext(chunkCapacity, sharedContext);
            nanoChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            nanoGetContext.close();
            nanoFillContext.close();
            nanoChunk.close();
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new ToTimeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ToTimeFillContext fillContext = (ToTimeFillContext) context;
        convertToTime(destination, nanoSource.getChunk(fillContext.nanoGetContext, rowSequence));
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ToTimeFillContext fillContext = (ToTimeFillContext) context;
        convertToTime(destination, nanoSource.getPrevChunk(fillContext.nanoGetContext, rowSequence));
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    @Override
    public void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        final ToTimeFillContext fillContext = (ToTimeFillContext) context;
        fillContext.nanoChunk.setSize(keys.size());
        nanoSource.fillChunkUnordered(fillContext.nanoFillContext, fillContext.nanoChunk, keys);
        convertToTime(dest, fillContext.nanoChunk);
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        final ToTimeFillContext fillContext = (ToTimeFillContext) context;
        fillContext.nanoChunk.setSize(keys.size());
        nanoSource.fillPrevChunkUnordered(fillContext.nanoFillContext, fillContext.nanoChunk, keys);
        convertToTime(dest, fillContext.nanoChunk);
    }

    private void convertToTime(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final LongChunk<? extends Values> nanoChunk) {
        final WritableObjectChunk<TIME_TYPE, ? super Values> timeDestination = destination.asWritableObjectChunk();
        final int size = nanoChunk.size();
        for (int ii = 0; ii < size; ++ii) {
            timeDestination.set(ii, makeValue(nanoChunk.get(ii)));
        }
        timeDestination.setSize(size);
    }

    private class ToNanosFillFromContext implements FillFromContext {
        private final FillFromContext nanoFillFromContext;
        private final WritableLongChunk<Values> nanoChunk;

        private ToNanosFillFromContext(final int chunkCapacity) {
            nanoFillFromContext = nanoSource.makeFillFromContext(chunkCapacity);
            nanoChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            nanoFillFromContext.close();
            nanoChunk.close();
        }

        /**
         * @return the nanoseconds of the first {@code size} values of {@code src}
         */
        private LongChunk<Values> convert(@NotNull final ObjectChunk<TIME_TYPE, ? extends Values> src, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                nanoChunk.set(ii, toNanos(src.get(ii)));
            }
            nanoChunk.setSize(size);
            return nanoChunk;
        }
    }

    @Override
    public FillFromContext makeFillFromContext(final int chunkCapacity) {
        return new ToNanosFillFromContext(chunkCapacity);
    }

    @Override
    public void fillFromChunk(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence) {
        final ToNanosFillFromContext fillFromContext = (ToNanosFillFromContext) context;
        nanoSource.fillFromChunk(fillFromContext.nanoFillFromContext,
                fillFromContext.convert(src.asObjectChunk(), rowSequence.intSize()), rowSequence);
    }

    @Override
    public void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys) {
        final ToNanosFillFromContext fillFromContext = (ToNanosFillFromContext) context;
        nanoSource.fillFromChunkUnordered(fillFromContext.nanoFillFromContext,
                fillFromContext.convert(src.asObjectChunk(), keys.size()), keys);
    }

    @Override
    public void setNull(final RowSequence rowSequence) {
        nanoSource.setNull(rowSequence);
    }
    // endregion

    // region Reinterpretation
    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class || alternateDataType == Instant.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (alternateDataType == this.getType()) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) this;
        } else if (alternateDataType == long.class || alternateDataType == Long.class) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) toEpochNano();
        } else if (alternateDataType == Instant.class) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) toInstant();
        }

        throw new IllegalArgumentException(
                "Cannot reinterpret `" + getType().getName() + "` to `" + alternateDataType.getName() + "`");
    }

    @Override
    public boolean supportsTimeConversion() {
        return true;
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {
        return new ZonedDateTimeSparseArraySource(Require.neqNull(zone, "zone"), nanoSource);
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {
        return new LongAsLocalDateColumnSource(nanoSource, zone);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {
        return new LongAsLocalTimeColumnSource(nanoSource, zone);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return new InstantSparseArraySource(nanoSource);
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return nanoSource;
    }
    // endregion
}
