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
import static io.deephaven.util.QueryConstants.NULL_BYTE;
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
 * If your size is smaller than Integer.MAX_VALUE, prefer {@link FlatByteArraySource}.
 */
public class Flat2DByteArraySource extends AbstractColumnSource<Byte> implements ImmutableColumnSourceGetDefaults.ForByte, WritableColumnSource<Byte>, FillUnordered, InMemoryColumnSource, ChunkedBackingStoreExposedWritableSource {
    private static final int DEFAULT_SEGMENT_SHIFT = 30;
    private final long segmentShift;
    private final int segmentMask;

    private final long size;
    private final byte[][] data;


    // region constructor
    public Flat2DByteArraySource(long size) {
        this(size, DEFAULT_SEGMENT_SHIFT);
    }

    public Flat2DByteArraySource(long size, int segmentShift) {
        super(byte.class);
        this.segmentShift = segmentShift;
        int segmentSize = 1 << segmentShift;
        segmentMask = segmentSize - 1;

        this.size = size;
        data = allocateArray(size, segmentSize);
    }
    // endregion constructor

    // region allocateArray
    private static byte [][] allocateArray(long size, int segmentSize) {
        final int segments = Math.toIntExact((size + segmentSize - 1) / segmentSize);
        final byte [][] data = new byte[segments][];
        int segment = 0;
        while (size > segmentSize) {
            data[segment++] = new byte[segmentSize];
            size -= segmentSize;
        }
        data[segment] = new byte[Math.toIntExact(size)];
        return data;
    }
    // endregion allocateArray

    @Override
    public final byte getByte(long index) {
        if (index < 0 || index >= size) {
            return NULL_BYTE;
        }

        return getUnsafe(index);
    }

    public int keyToSegment(long index) {
        return (int)(index >> segmentShift);
    }

    public int keyToOffset(long index) {
        return (int)(index & segmentMask);
    }

    public final byte getUnsafe(long key) {
        return data[keyToSegment(key)][keyToOffset(key)];
    }

    @Override
    public final void set(long key, byte value) {
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
        chunk.asResettableWritableByteChunk().resetFromTypedArray((byte[])data[segment], 0, data[segment].length);
        return (long)segment << segmentShift;
    }
    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        final int segment = keyToSegment(position);
        final int segmentLength = data[segment].length;
        final long firstPositionInSegment = (long)segment << segmentShift;
        final int offset = (int)(position - firstPositionInSegment);
        final int capacity = segmentLength - offset;
        chunk.asResettableWritableByteChunk().resetFromTypedArray((byte[])data[segment], offset, capacity);
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
        final WritableByteChunk<? super Values> asByteChunk = destination.asWritableByteChunk();
        final MutableInt destPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            while (start < end) {
                final int segment = keyToSegment(start);
                final int offset = keyToOffset(start);
                final long segmentEnd = start | segmentMask;
                final long realEnd = Math.min(segmentEnd, end);
                final int rangeLength = Math.toIntExact(realEnd - start + 1);
                asByteChunk.copyFromTypedArray(data[segment], offset, destPos.getAndAdd(rangeLength), rangeLength);
                start += rangeLength;
            }
        });
    }

    private void fillChunkByKeys(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableByteChunk<? super Values> asByteChunk = destination.asWritableByteChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> asByteChunk.set(srcPos.getAndIncrement(), getUnsafe(key)));
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return ByteChunk.getEmptyChunk();
        }
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        final GetContextWithResettable contextWithResettable = (GetContextWithResettable) context;
        return super.getChunk(contextWithResettable.inner, rowSequence);
    }

    private class GetContextWithResettable implements GetContext {
        final ResettableByteChunk<? extends Values> resettableByteChunk = ResettableByteChunk.makeResettableChunk();
        final GetContext inner;

        private GetContextWithResettable(GetContext inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            resettableByteChunk.close();
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
        return contextWithResettable.resettableByteChunk.resetFromTypedArray(data[segment], Math.toIntExact(firstKey), len);
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
        final ByteChunk<? extends Values> asByteChunk = src.asByteChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> set(key, asByteChunk.get(srcPos.getAndIncrement())));
    }

    private void fillFromChunkByRanges(Chunk<? extends Values> src, RowSequence rowSequence) {
        final ByteChunk<? extends Values> asByteChunk = src.asByteChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            while (start < end) {
                final int segment = keyToSegment(start);
                final int destOffset = keyToOffset(start);
                final long segmentEnd = start | segmentMask;
                final long realEnd = Math.min(segmentEnd, end);
                final int rangeLength = Math.toIntExact(realEnd - start + 1);
                asByteChunk.copyToTypedArray(srcPos.getAndAdd(rangeLength), data[segment], destOffset, rangeLength);
                start += rangeLength;
            }
        });
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        final ByteChunk<? extends Values> asByteChunk = src.asByteChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            set(keys.get(ii), asByteChunk.get(ii));
        }
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableByteChunk<? super Values> byteDest = dest.asWritableByteChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            final int key = Math.toIntExact(keys.get(ii));
            byteDest.set(ii, getUnsafe(key));
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
