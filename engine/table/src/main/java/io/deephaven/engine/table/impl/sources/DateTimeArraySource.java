/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Array-backed ColumnSource for DateTimes. Allows reinterpret as long.
 */
public class DateTimeArraySource extends AbstractLongArraySource<DateTime> {

    public DateTimeArraySource() {
        super(DateTime.class);
    }

    @Override
    public void setNull(long key) {
        set(key, NULL_LONG);
    }

    @Override
    public void set(long key, DateTime value) {
        set(key, value == null ? NULL_LONG : value.getNanos());
    }

    @Override
    public DateTime get(long rowKey) {
        final long nanos = getLong(rowKey);
        return DateTimeUtils.nanosToTime(nanos);
    }

    @Override
    public DateTime getPrev(long rowKey) {
        final long nanos = getPrevLong(rowKey);
        return DateTimeUtils.nanosToTime(nanos);
    }


    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class;
    }

    // the ArrayBackedColumnSource fillChunk can't handle changing the type
    @Override
    public void fillChunk(@NotNull ColumnSource.FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull RowSequence rowSequence) {
        final ChunkFiller filler = ChunkFiller.forChunkType(dest.getChunkType());
        if (rowSequence.getAverageRunLengthEstimate() > USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillByRanges(this, rowSequence, dest);
        } else {
            filler.fillByIndices(this, rowSequence, dest);
        }
    }

    @Override
    public void fillPrevChunk(@NotNull ColumnSource.FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull RowSequence rowSequence) {
        final ChunkFiller filler = ChunkFiller.forChunkType(dest.getChunkType());
        if (rowSequence.getAverageRunLengthEstimate() > USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillPrevByRanges(this, rowSequence, dest);
        } else {
            filler.fillPrevByIndices(this, rowSequence, dest);
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
    protected void fillSparseChunk(@NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final RowSequence indices) {
        super.fillSparseChunk(destGeneric, indices, DateTimeUtils::nanosToTime);
    }

    @Override
    protected void fillSparsePrevChunk(@NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final RowSequence indices) {
        super.fillSparsePrevChunk(destGeneric, indices, DateTimeUtils::nanosToTime);
    }

    @Override
    protected void fillSparseChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final LongChunk<? extends RowKeys> indices) {
        super.fillSparseChunkUnordered(destGeneric, indices, DateTimeUtils::nanosToTime);
    }

    @Override
    protected void fillSparsePrevChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final LongChunk<? extends RowKeys> indices) {
        super.fillSparsePrevChunkUnordered(destGeneric, indices, DateTimeUtils::nanosToTime);
    }

    @Override
    public void fillFromChunkByRanges(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        super.<DateTime>fillFromChunkByRanges(rowSequence, src, DateTimeUtils::nanos);
    }

    @Override
    void fillFromChunkByKeys(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        super.<DateTime>fillFromChunkByKeys(rowSequence, src, DateTimeUtils::nanos);
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull LongChunk<RowKeys> keys) {
        super.<DateTime>fillFromChunkUnordered(src, keys, DateTimeUtils::nanos);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        // noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) new ReinterpretedAsLong();
    }

    private class ReinterpretedAsLong extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong, FillUnordered<Values>, WritableColumnSource<Long> {
        private ReinterpretedAsLong() {
            super(long.class);
        }

        @Override
        public void startTrackingPrevValues() {
            DateTimeArraySource.this.startTrackingPrevValues();
        }

        @Override
        public long getLong(long rowKey) {
            return DateTimeArraySource.this.getLong(rowKey);
        }

        @Override
        public long getPrevLong(long rowKey) {
            return DateTimeArraySource.this.getPrevLong(rowKey);
        }

        @Override
        public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return alternateDataType == DateTime.class;
        }

        @Override
        protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
                @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) DateTimeArraySource.this;
        }

        @Override
        public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
            return DateTimeArraySource.super.makeFillContext(getChunkType());
        }

        @Override
        public void fillChunk(@NotNull final ColumnSource.FillContext context,
                @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
            // can't defer this case to super as they will ultimately call a method on DateTimeArraySource instead of
            // AbstractLongArraySource
            if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
                fillSparseLongChunk(destination, rowSequence);
            } else {
                DateTimeArraySource.super.fillChunk(context, destination, rowSequence);
            }
        }

        @Override
        public void fillPrevChunk(@NotNull final ColumnSource.FillContext context,
                @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
            // can't defer these two cases to super as they will ultimately call a method on DateTimeArraySource instead
            // of AbstractLongArraySource
            if (prevFlusher == null) {
                fillChunk(context, destination, rowSequence);
                return;
            }

            if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
                fillSparsePrevLongChunk(destination, rowSequence);
                return;
            }

            DateTimeArraySource.super.fillPrevChunk(context, destination, rowSequence);
        }

        @Override
        public void fillChunkUnordered(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final LongChunk<? extends RowKeys> keyIndices) {
            fillSparseLongChunkUnordered(destination, keyIndices);
        }

        @Override
        public void fillPrevChunkUnordered(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final LongChunk<? extends RowKeys> keyIndices) {
            fillSparsePrevLongChunkUnordered(destination, keyIndices);
        }

        @Override
        public void setNull(long key) {
            set(key, NULL_LONG);
        }

        @Override
        public void set(long key, long value) {
            DateTimeArraySource.super.set(key, value);
        }

        @Override
        public void ensureCapacity(long capacity, boolean nullFill) {
            DateTimeArraySource.this.ensureCapacity(capacity, nullFill);
        }

        @Override
        public FillFromContext makeFillFromContext(int chunkCapacity) {
            return DateTimeArraySource.super.makeFillFromContext(chunkCapacity);
        }

        @Override
        public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
                @NotNull RowSequence rowSequence) {
            // Note: we cannot call super.fillFromChunk here as that method will call the override versions that expect
            // ObjectChunks.
            if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
                DateTimeArraySource.super.fillFromChunkByKeys(rowSequence, src);
            } else {
                DateTimeArraySource.super.fillFromChunkByRanges(rowSequence, src);
            }
        }

        @Override
        public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
                @NotNull LongChunk<RowKeys> keys) {
            DateTimeArraySource.super.fillFromChunkUnordered(context, src, keys);
        }

        @Override
        public boolean providesFillUnordered() {
            return true;
        }
    }

    @Override
    public boolean exposesChunkedBackingStore() {
        // our backing store is not a DateTime chunk
        return false;
    }
}
