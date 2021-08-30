/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.util.chunkfillers.ChunkFiller;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Array-backed ColumnSource for DBDateTimes. Allows reinterpret as long.
 */
public class DateTimeArraySource extends AbstractLongArraySource<DBDateTime> {

    public DateTimeArraySource() {
        super(DBDateTime.class);
    }

    @Override
    public void set(long key, DBDateTime value) {
        set(key, value == null ? NULL_LONG : value.getNanos());
    }

    @Override
    public DBDateTime get(long index) {
        final long nanos = getLong(index);
        return DBTimeUtils.nanosToTime(nanos);
    }

    @Override
    public DBDateTime getPrev(long index) {
        final long nanos = getPrevLong(index);
        return DBTimeUtils.nanosToTime(nanos);
    }


    @Override
    public void copy(ColumnSource<? extends DBDateTime> sourceColumn, long sourceKey, long destKey) {
        set(destKey, sourceColumn.get(sourceKey));
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class;
    }

    // the ArrayBackedColumnSource fillChunk can't handle changing the type
    @Override
    public void fillChunk(@NotNull ColumnSource.FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull OrderedKeys orderedKeys) {
        final ChunkFiller filler = dest.getChunkFiller();
        if (orderedKeys.getAverageRunLengthEstimate() > USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillByRanges(this, orderedKeys, dest);
        } else {
            filler.fillByIndices(this, orderedKeys, dest);
        }
    }

    @Override
    public void fillPrevChunk(@NotNull ColumnSource.FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull OrderedKeys orderedKeys) {
        final ChunkFiller filler = dest.getChunkFiller();
        if (orderedKeys.getAverageRunLengthEstimate() > USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillPrevByRanges(this, orderedKeys, dest);
        } else {
            filler.fillPrevByIndices(this, orderedKeys, dest);
        }
    }

    @Override
    public Chunk<Values> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        return getChunkByFilling(context, orderedKeys);
    }

    @Override
    public Chunk<Values> getPrevChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        return getPrevChunkByFilling(context, orderedKeys);
    }

    @Override
    protected void fillSparseChunk(@NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final OrderedKeys indices) {
        super.fillSparseChunk(destGeneric, indices, DBTimeUtils::nanosToTime);
    }

    @Override
    protected void fillSparsePrevChunk(@NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final OrderedKeys indices) {
        super.fillSparsePrevChunk(destGeneric, indices, DBTimeUtils::nanosToTime);
    }

    @Override
    protected void fillSparseChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final LongChunk<? extends Attributes.KeyIndices> indices) {
        super.fillSparseChunkUnordered(destGeneric, indices, DBTimeUtils::nanosToTime);
    }

    @Override
    protected void fillSparsePrevChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final LongChunk<? extends Attributes.KeyIndices> indices) {
        super.fillSparsePrevChunkUnordered(destGeneric, indices, DBTimeUtils::nanosToTime);
    }

    @Override
    public void fillFromChunkByRanges(@NotNull OrderedKeys orderedKeys, Chunk<? extends Values> src) {
        super.<DBDateTime>fillFromChunkByRanges(orderedKeys, src, DBTimeUtils::nanos);
    }

    @Override
    void fillFromChunkByKeys(@NotNull OrderedKeys orderedKeys, Chunk<? extends Values> src) {
        super.<DBDateTime>fillFromChunkByKeys(orderedKeys, src, DBTimeUtils::nanos);
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull LongChunk<Attributes.KeyIndices> keys) {
        super.<DBDateTime>fillFromChunkUnordered(src, keys, DBTimeUtils::nanos);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        // noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) new ReinterpretedAsLong();
    }

    private class ReinterpretedAsLong extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong, FillUnordered, WritableSource<Long> {
        private ReinterpretedAsLong() {
            super(long.class);
        }

        @Override
        public void startTrackingPrevValues() {
            DateTimeArraySource.this.startTrackingPrevValues();
        }

        @Override
        public long getLong(long index) {
            return DateTimeArraySource.this.getLong(index);
        }

        @Override
        public long getPrevLong(long index) {
            return DateTimeArraySource.this.getPrevLong(index);
        }

        @Override
        public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return alternateDataType == DBDateTime.class;
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
                @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
            // can't defer this case to super as they will ultimately call a method on DateTimeArraySource instead of
            // AbstractLongArraySource
            if (orderedKeys.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
                fillSparseLongChunk(destination, orderedKeys);
            } else {
                DateTimeArraySource.super.fillChunk(context, destination, orderedKeys);
            }
        }

        @Override
        public void fillPrevChunk(@NotNull final ColumnSource.FillContext context,
                @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
            // can't defer these two cases to super as they will ultimately call a method on DateTimeArraySource instead
            // of AbstractLongArraySource
            if (prevFlusher == null) {
                fillChunk(context, destination, orderedKeys);
                return;
            }

            if (orderedKeys.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
                fillSparsePrevLongChunk(destination, orderedKeys);
                return;
            }

            DateTimeArraySource.super.fillPrevChunk(context, destination, orderedKeys);
        }

        @Override
        public void fillChunkUnordered(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final LongChunk<? extends Attributes.KeyIndices> keyIndices) {
            fillSparseLongChunkUnordered(destination, keyIndices);
        }

        @Override
        public void fillPrevChunkUnordered(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final LongChunk<? extends Attributes.KeyIndices> keyIndices) {
            fillSparsePrevLongChunkUnordered(destination, keyIndices);
        }

        @Override
        public void set(long key, long value) {
            DateTimeArraySource.super.set(key, value);
        }

        public void copy(ColumnSource<? extends Long> sourceColumn, long sourceKey, long destKey) {
            DateTimeArraySource.super.set(destKey, sourceColumn.getLong(sourceKey));
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
                @NotNull OrderedKeys orderedKeys) {
            // Note: we cannot call super.fillFromChunk here as that method will call the override versions that expect
            // ObjectChunks.
            if (orderedKeys.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
                DateTimeArraySource.super.fillFromChunkByKeys(orderedKeys, src);
            } else {
                DateTimeArraySource.super.fillFromChunkByRanges(orderedKeys, src);
            }
        }

        @Override
        public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
                @NotNull LongChunk<Attributes.KeyIndices> keys) {
            DateTimeArraySource.super.fillFromChunkUnordered(context, src, keys);
        }
    }

}
