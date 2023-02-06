/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.*;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import java.util.Arrays;

// region boxing imports
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
// endregion boxing imports

/**
 * Simple flat array source that supports fillFromChunk for initial creation.
 *
 * No previous value tracking is permitted, so this column source is only useful as a flat static source.
 *
 * A single array backs the result, so getChunk calls with contiguous ranges should always be able to return a
 * reference to the backing store without an array copy.  The immediate consequence is that you may not create
 * sources that have a capacity larger than the maximum capacity of an array.
 *
 * If your size is greater than the maximum capacity of an array, prefer {@link Immutable2DFloatArraySource}.
 */
public class ImmutableFloatArraySource extends AbstractDeferredGroupingColumnSource<Float> implements ImmutableColumnSourceGetDefaults.ForFloat, WritableColumnSource<Float>, FillUnordered<Values>, InMemoryColumnSource, ChunkedBackingStoreExposedWritableSource, WritableSourceWithPrepareForParallelPopulation {
    private float[] data;

    // region constructor
    public ImmutableFloatArraySource() {
        super(float.class);
    }
    // endregion constructor

    // region array constructor
    public ImmutableFloatArraySource(float [] data) {
        super(float.class);
        this.data = data;
    }
    // endregion array constructor

    // region allocateArray
    void allocateArray(long capacity, boolean nullFilled) {
        final int intCapacity = Math.toIntExact(capacity);
        this.data = new float[intCapacity];
        if (nullFilled) {
            Arrays.fill(this.data, 0, intCapacity, NULL_FLOAT);
        }
    }
    // endregion allocateArray

    @Override
    public final float getFloat(long rowKey) {
        if (rowKey < 0 || rowKey >= data.length) {
            return NULL_FLOAT;
        }

        return getUnsafe(rowKey);
    }

    public final float getUnsafe(long index) {
        return data[(int)index];
    }

    public final float getAndSetUnsafe(long index, float newValue) {
        float oldValue = data[(int)index];
        data[(int)index] = newValue;
        return oldValue;
    }

    @Override
    public final void setNull(long key) {
        data[(int)key] = NULL_FLOAT;
    }

    @Override
    public final void set(long key, float value) {
        data[(int)key] = value;
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        if (data == null) {
            allocateArray(capacity, nullFilled);
        }
        if (capacity > data.length) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() >= ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillChunkByRanges(destination, rowSequence);
        } else {
            fillChunkByKeys(destination, rowSequence);
        }
    }

    private void fillChunkByRanges(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableFloatChunk<? super Values> asFloatChunk = destination.asWritableFloatChunk();
        final MutableInt destPosition = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            final int rangeLength = (int)(end - start + 1);
            asFloatChunk.copyFromTypedArray(data, (int)start, destPosition.getAndAdd(rangeLength), rangeLength);
        });
        asFloatChunk.setSize(destPosition.intValue());
    }

    private void fillChunkByKeys(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableFloatChunk<? super Values> asFloatChunk = destination.asWritableFloatChunk();
        final MutableInt destPosition = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> asFloatChunk.set(destPosition.getAndIncrement(), getUnsafe(key)));
        asFloatChunk.setSize(destPosition.intValue());
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return FloatChunk.getEmptyChunk();
        }
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        return super.getChunk(context, rowSequence);
    }

    @Override
    public long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position) {
        chunk.asResettableWritableFloatChunk().resetFromTypedArray(data, 0, data.length);
        return 0;
    }

    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        final int capacity = (int)(data.length - position);
        ResettableWritableFloatChunk resettableWritableFloatChunk = chunk.asResettableWritableFloatChunk();
        resettableWritableFloatChunk.resetFromTypedArray(data, (int)position, capacity);
        return capacity;
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int len = (int)(lastKey - firstKey + 1);
        //noinspection unchecked
        DefaultGetContext<? extends Values> context1 = (DefaultGetContext<? extends Values>) context;
        return context1.getResettableChunk().resetFromArray(data, (int)firstKey, len);
    }

    @Override
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() >= ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillFromChunkByRanges(src, rowSequence);
        } else {
            fillFromChunkByKeys(src, rowSequence);
        }
    }

    private void fillFromChunkByKeys(Chunk<? extends Values> src, RowSequence rowSequence) {
        final FloatChunk<? extends Values> asFloatChunk = src.asFloatChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> set(key, asFloatChunk.get(srcPos.getAndIncrement())));
    }

    private void fillFromChunkByRanges(Chunk<? extends Values> src, RowSequence rowSequence) {
        final FloatChunk<? extends Values> asFloatChunk = src.asFloatChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            final int rangeLength = (int)(end - start + 1);
            asFloatChunk.copyToTypedArray(srcPos.getAndAdd(rangeLength), data, (int)start, rangeLength);
        });
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        final FloatChunk<? extends Values> asFloatChunk = src.asFloatChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            set(keys.get(ii), asFloatChunk.get(ii));
        }
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableFloatChunk<? super Values> floatDest = dest.asWritableFloatChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            final long longKey = keys.get(ii);
            if (longKey == RowSet.NULL_ROW_KEY) {
                floatDest.set(ii, NULL_FLOAT);
            } else {
                final int key = (int)longKey;
                floatDest.set(ii, getUnsafe(key));
            }
        }
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        fillChunkUnordered(context, dest, keys);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
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
    public void prepareForParallelPopulation(RowSet rowSet) {
        // we don't track previous values, so we don't care to do any work
    }

    // region getArray
    public float [] getArray() {
        return data;
    }
    // endregion getArray

    // region setArray
    public void setArray(float [] array) {
        data = array;
    }
    // endregion setArray

    // region reinterpret
    // endregion reinterpret
}
