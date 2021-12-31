/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit Flat2DCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.flat;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ChunkedBackingStoreExposedWritableSource;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

// region boxing imports
// endregion boxing imports

/**
 * Simple almost flat array source that supports fillFromChunk for initial creation.
 *
 * No previous value tracking is permitted, so this column source is only useful as a flat static source.
 *
 * A two-dimension array single array backs the result, with by default segments of 2^30 elements.  This is so that
 * getChunk calls with contiguous ranges are often able to return a reference to the backing store without an array
 * copy.
 *
 * If your size is smaller than Integer.MAX_VALUE, prefer {@link FlatObjectArraySource}.
 */
public class Flat2DObjectArraySource<T> extends AbstractColumnSource<T> implements ImmutableColumnSourceGetDefaults.ForObject<T>, WritableColumnSource<T>, FillUnordered, InMemoryColumnSource, ChunkedBackingStoreExposedWritableSource {
    private static final int DEFAULT_SEGMENT_SHIFT = 30;
    private final long segmentShift;
    private final int segmentMask;

    private final long size;
    private final Object[][] data;


    // region constructor
    public Flat2DObjectArraySource(Class<T> type, Class<?> componentType, long size) {
        this(type, componentType, size, DEFAULT_SEGMENT_SHIFT);
    }

    public Flat2DObjectArraySource(Class<T> type, Class<?> componentType, long size, int segmentShift) {
        super(type, componentType);
        this.segmentShift = segmentShift;
        int segmentSize = 1 << segmentShift;
        segmentMask = segmentSize - 1;

        this.size = size;
        data = allocateArray(size, segmentSize);
    }
    // endregion constructor

    // region allocateArray
    private static Object [][] allocateArray(long size, int segmentSize) {
        final int segments = Math.toIntExact((size + segmentSize - 1) / segmentSize);
        final Object [][] data = new Object[segments][];
        int segment = 0;
        while (size > segmentSize) {
            data[segment++] = new Object[segmentSize];
            size -= segmentSize;
        }
        data[segment] = new Object[Math.toIntExact(size)];
        return data;
    }
    // endregion allocateArray

    @Override
    public final T get(long index) {
        if (index < 0 || index >= size) {
            return null;
        }

        return getUnsafe(index);
    }

    public int keyToSegment(long index) {
        return (int)(index >> segmentShift);
    }

    public int keyToOffset(long index) {
        return (int)(index & segmentMask);
    }

    public final T getUnsafe(long key) {
        return (T)data[keyToSegment(key)][keyToOffset(key)];
    }

    @Override
    public final void set(long key, Object value) {
        data[keyToSegment(key)][keyToOffset(key)] = value;
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        if (capacity > size) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position) {
        final int segment = keyToSegment(position);
        chunk.asResettableWritableObjectChunk().resetFromTypedArray((Object[])data[segment], 0, data[segment].length);
        return (long)segment << segmentShift;
    }
    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        final int segment = keyToSegment(position);
        final int segmentLength = data[segment].length;
        final long firstPositionInSegment = (long)segment << segmentShift;
        final int offset = (int)(position - firstPositionInSegment);
        final int capacity = segmentLength - offset;
        chunk.asResettableWritableObjectChunk().resetFromTypedArray((Object[])data[segment], offset, capacity);
        return capacity;
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
        final WritableObjectChunk<T, ? super Values> asObjectChunk = destination.asWritableObjectChunk();
        final MutableInt destPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            while (start < end) {
                final int segment = keyToSegment(start);
                final int offset = keyToOffset(start);
                final long segmentEnd = start | segmentMask;
                final long realEnd = Math.min(segmentEnd, end);
                final int rangeLength = Math.toIntExact(realEnd - start + 1);
                asObjectChunk.copyFromTypedArray((T[])data[segment], offset, destPos.getAndAdd(rangeLength), rangeLength);
                start += rangeLength;
            }
        });
    }

    private void fillChunkByKeys(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableObjectChunk<T, ? super Values> asObjectChunk = destination.asWritableObjectChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> asObjectChunk.set(srcPos.getAndIncrement(), getUnsafe(key)));
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return ObjectChunk.getEmptyChunk();
        }
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        final GetContextWithResettable contextWithResettable = (GetContextWithResettable) context;
        return super.getChunk(contextWithResettable.inner, rowSequence);
    }

    private class GetContextWithResettable implements GetContext {
        final ResettableObjectChunk<T, ? extends Values> resettableObjectChunk = ResettableObjectChunk.makeResettableChunk();
        final GetContext inner;

        private GetContextWithResettable(GetContext inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            resettableObjectChunk.close();
            inner.close();
        }
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return new GetContextWithResettable(super.makeGetContext(chunkCapacity, sharedContext));
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final GetContextWithResettable contextWithResettable = (GetContextWithResettable) context;
        final int segment = keyToSegment(firstKey);
        if (segment != keyToSegment(lastKey)) {
            super.getChunk(contextWithResettable.inner, firstKey, lastKey);
        }
        final int len = Math.toIntExact(lastKey - firstKey + 1);
        return contextWithResettable.resettableObjectChunk.resetFromTypedArray((T[])data[segment], Math.toIntExact(firstKey), len);
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
        final ObjectChunk<T, ? extends Values> asObjectChunk = src.asObjectChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> set(key, asObjectChunk.get(srcPos.getAndIncrement())));
    }

    private void fillFromChunkByRanges(Chunk<? extends Values> src, RowSequence rowSequence) {
        final ObjectChunk<T, ? extends Values> asObjectChunk = src.asObjectChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            while (start < end) {
                final int segment = keyToSegment(start);
                final int destOffset = keyToOffset(start);
                final long segmentEnd = start | segmentMask;
                final long realEnd = Math.min(segmentEnd, end);
                final int rangeLength = Math.toIntExact(realEnd - start + 1);
                asObjectChunk.copyToTypedArray(srcPos.getAndAdd(rangeLength), (T[])data[segment], destOffset, rangeLength);
                start += rangeLength;
            }
        });
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        final ObjectChunk<T, ? extends Values> asObjectChunk = src.asObjectChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            set(keys.get(ii), asObjectChunk.get(ii));
        }
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableObjectChunk<T, ? super Values> ObjectDest = dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            final int key = Math.toIntExact(keys.get(ii));
            ObjectDest.set(ii, getUnsafe(key));
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
}
