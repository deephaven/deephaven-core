/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

// region boxing imports

// endregion boxing imports

public abstract class Immutable2DNanosBasedTimeArraySource<TIME_TYPE>
        extends AbstractDeferredGroupingColumnSource<TIME_TYPE>
        implements WritableColumnSource<TIME_TYPE>, FillUnordered<Values>, InMemoryColumnSource, ConvertableTimeSource,
        WritableSourceWithPrepareForParallelPopulation {

    protected final Immutable2DLongArraySource nanoSource;

    // region constructor
    public Immutable2DNanosBasedTimeArraySource(
            final @NotNull Class<TIME_TYPE> type) {
        super(type);
        this.nanoSource = new Immutable2DLongArraySource();
    }

    public Immutable2DNanosBasedTimeArraySource(
            final @NotNull Class<TIME_TYPE> type,
            final Immutable2DLongArraySource nanoSource) {
        super(type);
        this.nanoSource = nanoSource;
    }
    // endregion constructor

    // region Getters & Setters
    protected abstract TIME_TYPE makeValue(final long nanos);

    protected abstract long toNanos(final TIME_TYPE value);

    @Override
    public TIME_TYPE get(long rowKey) {
        return makeValue(getLong(rowKey));
    }

    @Override
    public TIME_TYPE getPrev(long rowKey) {
        return makeValue(getPrevLong(rowKey));
    }

    @Override
    public final long getLong(long rowKey) {
        return nanoSource.getLong(rowKey);
    }

    @Override
    public final void setNull(long key) {
        nanoSource.setNull(key);
    }

    @Override
    public final void set(long key, long value) {
        nanoSource.set(key, value);
    }
    // endregion Getters & Setters

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        nanoSource.ensureCapacity(capacity, nullFilled);
    }

    @Override
    public FillFromContext makeFillFromContext(int chunkCapacity) {
        return nanoSource.makeFillFromContext(chunkCapacity);
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return nanoSource.makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() >= ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillChunkByRanges(destination, rowSequence);
        } else {
            fillChunkByKeys(destination, rowSequence);
        }
    }

    private void fillChunkByRanges(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        nanoSource.fillChunkByRanges(destination, rowSequence, this::makeValue);
    }

    private void fillChunkByKeys(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        nanoSource.fillChunkByKeys(destination, rowSequence, this::makeValue);
    }

    @Override
    public void fillFromChunk(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() >= ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillFromChunkByRanges(src, rowSequence);
        } else {
            fillFromChunkByKeys(src, rowSequence);
        }
    }

    private void fillFromChunkByKeys(
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence) {
        nanoSource.fillFromChunkByKeys(src, rowSequence, this::toNanos);
    }

    private void fillFromChunkByRanges(
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence) {
        nanoSource.fillFromChunkByRanges(src, rowSequence, this::toNanos);
    }

    @Override
    public void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys) {
        nanoSource.fillFromChunkUnordered(context, src, keys, this::toNanos);
    }

    @Override
    public void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        nanoSource.fillChunkUnordered(context, dest, keys, this::makeValue);
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        fillChunkUnordered(context, dest, keys);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        fillChunk(context, destination, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return getChunk(context, firstKey, lastKey);
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    @Override
    public void prepareForParallelPopulation(RowSequence rowSequence) {
        nanoSource.prepareForParallelPopulation(rowSequence);
    }

    // region reinterpretation
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
        return new Immutable2DZonedDateTimeArraySource(Require.neqNull(zone, "zone"), nanoSource);
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
        return new Immutable2DDateTimeArraySource(nanoSource);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return new Immutable2DInstantArraySource(nanoSource);
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return nanoSource;
    }
    // endregion reinterpretation
}
