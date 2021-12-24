/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit Flat2DCharArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
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
import static io.deephaven.util.QueryConstants.NULL_LONG;
// endregion boxing imports

/**
 * Simple flat array source that supports filling for initial creation.
 */
public class Flat2DLongArraySource extends AbstractColumnSource<Long> implements ImmutableColumnSourceGetDefaults.ForLong, WritableColumnSource<Long>, FillUnordered, InMemoryColumnSource, ChunkedBackingStoreExposedWritableSource {
    private static final long SEGMENT_SHIFT = 30;
    private static final int SEGMENT_SIZE = 1<<SEGMENT_SHIFT;
    private static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

    private final long size;
    private final long[][] data;

    // region constructor
    public Flat2DLongArraySource(long size) {
        super(long.class);
        this.size = size;
        data = allocateArray(size);
    }
    // endregion constructor

    // region allocateArray
    private static long [][] allocateArray(long size) {
        final int segments = Math.toIntExact((size + SEGMENT_SIZE - 1) / SEGMENT_SIZE);
        final long [][] data = new long[segments][];
        int segment = 0;
        while (size > SEGMENT_SIZE) {
            data[segment++] = new long[SEGMENT_SIZE];
            size -= SEGMENT_SIZE;
        }
        data[segment] = new long[Math.toIntExact(size)];
        return data;
    }
    // endregion allocateArray

    @Override
    public final long getLong(long index) {
        if (index < 0 || index >= size) {
            return NULL_LONG;
        }

        return getUnsafe(index);
    }

    public int keyToSegment(long index) {
        return (int)(index >> SEGMENT_SHIFT);
    }

    public int keyToOffset(long index) {
        return (int)(index & SEGMENT_MASK);
    }

    public final long getUnsafe(long key) {
        return data[keyToSegment(key)][keyToOffset(key)];
    }

    @Override
    public final void set(long key, long value) {
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
        chunk.asResettableWritableLongChunk().resetFromTypedArray((long[])data[segment], 0, data[segment].length);
        return (long)segment << SEGMENT_SHIFT;
    }
    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        final int segment = keyToSegment(position);
        final int segmentLength = data[segment].length;
        final long firstPositionInSegment = (long)segment << SEGMENT_SHIFT;
        final int offset = (int)(position - firstPositionInSegment);
        final int capacity = segmentLength - offset;
        chunk.asResettableWritableLongChunk().resetFromTypedArray((long[])data[segment], offset, capacity);
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
        final WritableLongChunk<? super Values> asLongChunk = destination.asWritableLongChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            while (start < end) {
                final int segment = keyToSegment(start);
                final long segmentEnd = start | SEGMENT_MASK;
                final long realEnd = Math.min(segmentEnd, end);
                final int rangeLength = Math.toIntExact(realEnd - start + 1);
                asLongChunk.copyFromTypedArray(data[segment], Math.toIntExact(start), srcPos.getAndAdd(rangeLength), rangeLength);
                start += rangeLength;
            }
        });
    }

    private void fillChunkByKeys(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableLongChunk<? super Values> asLongChunk = destination.asWritableLongChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> asLongChunk.set(srcPos.getAndIncrement(), getUnsafe(key)));
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        final GetContextWithResettable contextWithResettable = (GetContextWithResettable) context;
        return super.getChunk(contextWithResettable.inner, rowSequence);
    }

    private class GetContextWithResettable implements GetContext {
        final ResettableLongChunk<? extends Values> resettableLongChunk = ResettableLongChunk.makeResettableChunk();
        final GetContext inner;

        private GetContextWithResettable(GetContext inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            resettableLongChunk.close();
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
        return contextWithResettable.resettableLongChunk.resetFromTypedArray(data[segment], Math.toIntExact(firstKey), len);
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
        final LongChunk<? extends Values> asLongChunk = src.asLongChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> set(key, asLongChunk.get(srcPos.getAndIncrement())));
    }

    private void fillFromChunkByRanges(Chunk<? extends Values> src, RowSequence rowSequence) {
        final LongChunk<? extends Values> asLongChunk = src.asLongChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            while (start < end) {
                final int segment = keyToSegment(start);
                final long segmentEnd = start | SEGMENT_MASK;
                final long realEnd = Math.min(segmentEnd, end);
                final int rangeLength = Math.toIntExact(realEnd - start + 1);
                asLongChunk.copyToTypedArray(srcPos.getAndAdd(rangeLength), data[segment], Math.toIntExact(start), rangeLength);
                start += rangeLength;
            }
        });
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        final LongChunk<? extends Values> asLongChunk = src.asLongChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            set(keys.get(ii), asLongChunk.get(ii));
        }
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableLongChunk<? super Values> longDest = dest.asWritableLongChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            final int key = Math.toIntExact(keys.get(ii));
            longDest.set(ii, getUnsafe(key));
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
