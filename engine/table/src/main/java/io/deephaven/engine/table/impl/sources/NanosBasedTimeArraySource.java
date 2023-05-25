/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.util.ShiftData;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;

import java.time.*;

public abstract class NanosBasedTimeArraySource<TIME_TYPE> extends AbstractDeferredGroupingColumnSource<TIME_TYPE>
        implements FillUnordered<Values>, ShiftData.ShiftCallback, WritableColumnSource<TIME_TYPE>,
        InMemoryColumnSource, WritableSourceWithPrepareForParallelPopulation, ConvertableTimeSource {

    protected final LongArraySource nanoSource;

    public NanosBasedTimeArraySource(final @NotNull Class<TIME_TYPE> type) {
        this(type, new LongArraySource());
    }

    public NanosBasedTimeArraySource(final @NotNull Class<TIME_TYPE> type, final @NotNull LongArraySource nanoSource) {
        super(type);
        this.nanoSource = nanoSource;
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

    public final long getAndSetUnsafe(long rowKey, long newValue) {
        return nanoSource.getAndSetUnsafe(rowKey, newValue);
    }

    @Override
    public void shift(long start, long end, long offset) {
        nanoSource.shift(start, end, offset);
    }
    // endregion

    // region ArraySource impl
    @Override
    public void startTrackingPrevValues() {
        nanoSource.startTrackingPrevValues();
    }

    @Override
    public void ensureCapacity(long size, boolean nullFill) {
        nanoSource.ensureCapacity(size, nullFill);
    }

    @Override
    public void prepareForParallelPopulation(RowSequence rowSequence) {
        nanoSource.prepareForParallelPopulation(rowSequence);
    }
    // endregion

    // region Chunking
    @Override
    public void fillChunk(@NotNull ChunkSource.FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull RowSequence rowSequence) {
        nanoSource.fillChunk(context, dest, rowSequence, this::makeValue);
    }

    @Override
    public void fillPrevChunk(
            @NotNull ColumnSource.FillContext context,
            @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            nanoSource.fillSparsePrevChunk(destination, rowSequence, this::makeValue);
        } else {
            nanoSource.fillPrevChunk(context, destination, rowSequence, this::makeValue);
        }
    }

    @Override
    public Chunk<Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return getChunkByFilling(context, rowSequence);
    }

    @Override
    public Chunk<Values> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return getPrevChunkByFilling(context, rowSequence);
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
        nanoSource.fillSparseChunkUnordered(dest, keys, this::makeValue);
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        nanoSource.fillSparsePrevChunkUnordered(dest, keys, this::makeValue);
    }

    @Override
    public void fillFromChunkUnordered(
            @NotNull FillFromContext context,
            @NotNull Chunk<? extends Values> src,
            @NotNull LongChunk<RowKeys> keys) {
        nanoSource.fillFromChunkUnordered(context, src, keys, this::toNanos);
    }
    // endregion

    // region Reinterpretation
    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class || alternateDataType == Instant.class
                || alternateDataType == DateTime.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (alternateDataType == this.getType()) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) this;
        } else if (alternateDataType == DateTime.class) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) toDateTime();
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
    public ColumnSource<ZonedDateTime> toZonedDateTime(final @NotNull ZoneId zone) {
        return new ZonedDateTimeArraySource(Require.neqNull(zone, "zone"), nanoSource);
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(final @NotNull ZoneId zone) {
        return new LocalDateWrapperSource(toZonedDateTime(zone), zone);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(final @NotNull ZoneId zone) {
        return new LocalTimeWrapperSource(toZonedDateTime(zone), zone);
    }

    @Override
    public ColumnSource<DateTime> toDateTime() {
        return new DateTimeArraySource(nanoSource);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return new InstantArraySource(nanoSource);
    }

    @Override
    public LongArraySource toEpochNano() {
        return nanoSource;
    }
    // endregion
}
