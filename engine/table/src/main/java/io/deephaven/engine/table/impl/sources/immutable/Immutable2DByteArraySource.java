/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit Immutable2DCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.ColumnSource;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
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
 * If your size is smaller than the maximum array size, prefer {@link ImmutableByteArraySource}.
 */
public class Immutable2DByteArraySource extends AbstractDeferredGroupingColumnSource<Byte> implements ImmutableColumnSourceGetDefaults.ForByte, WritableColumnSource<Byte>, FillUnordered<Values>, InMemoryColumnSource, ChunkedBackingStoreExposedWritableSource, WritableSourceWithPrepareForParallelPopulation {
    private static final int DEFAULT_SEGMENT_SHIFT = 30;
    private final long segmentShift;
    private final int segmentMask;

    private long size;
    private byte[][] data;


    // region constructor
    public Immutable2DByteArraySource() {
        this(DEFAULT_SEGMENT_SHIFT);
    }

    public Immutable2DByteArraySource(int segmentShift) {
        super(byte.class);
        this.segmentShift = segmentShift;
        int segmentSize = 1 << segmentShift;
        segmentMask = segmentSize - 1;
    }
    // endregion constructor

    // region allocateArray
    private static byte [][] allocateArray(long size, int segmentSize, boolean nullFilled) {
        final int segments = Math.toIntExact((size + segmentSize - 1) / segmentSize);
        final byte [][] data = new byte[segments][];
        int segment = 0;
        while (size > 0) {
            final int thisSegmentSize = (int)Math.min(segmentSize, size);
            data[segment] = new byte[thisSegmentSize];
            if (nullFilled) {
                Arrays.fill(data[segment], 0, thisSegmentSize, NULL_BYTE);
            }
            segment++;
            size -= thisSegmentSize;
        }
        return data;
    }
    // endregion allocateArray

    @Override
    public final byte getByte(long rowKey) {
        if (rowKey < 0 || rowKey >= size) {
            return NULL_BYTE;
        }

        return getUnsafe(rowKey);
    }

    private int keyToSegment(long index) {
        return (int)(index >> segmentShift);
    }

    private int keyToOffset(long index) {
        return (int)(index & segmentMask);
    }

    public final byte getUnsafe(long key) {
        return data[keyToSegment(key)][keyToOffset(key)];
    }

    @Override
    public final void setNull(long key) {
        data[keyToSegment(key)][keyToOffset(key)] = NULL_BYTE;
    }

    @Override
    public final void set(long key, byte value) {
        data[keyToSegment(key)][keyToOffset(key)] = value;
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        if (data == null) {
            size = capacity;
            data = allocateArray(size, segmentMask + 1, nullFilled);
        }
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
        final int offset = keyToOffset(position);
        final int segmentLength = data[segment].length;
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
                final int rangeLength = (int)(realEnd - start + 1);
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
        return super.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int segment = keyToSegment(firstKey);
        if (segment != keyToSegment(lastKey)) {
            // the super will just go into our getChunk with RowSequence and that can be an infinite loop
            try (final RowSequence rs = RowSequenceFactory.forRange(firstKey, lastKey)) {
                return super.getChunk(context, rs);
            }
        }
        final int len = (int)(lastKey - firstKey + 1);
        final int firstOffset = keyToOffset(firstKey);
        //noinspection unchecked
        DefaultGetContext<? extends Values> context1 = (DefaultGetContext<? extends Values>) context;
        return context1.getResettableChunk().resetFromArray(data[segment], firstOffset, len);
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
                final int rangeLength = (int)(realEnd - start + 1);
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
            final long rowKey = keys.get(ii);
            if (rowKey == RowSequence.NULL_ROW_KEY) {
                byteDest.set(ii, NULL_BYTE);
            } else {
                byteDest.set(ii, getUnsafe((int)(rowKey)));
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
        // nothing to do
    }

    // region reinterpret
    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == Boolean.class;
    }

    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
               @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
         //noinspection unchecked
         return (ColumnSource<ALTERNATE_DATA_TYPE>) new ByteAsBooleanColumnSource(this);
    }
    // endregion reinterpret
}
