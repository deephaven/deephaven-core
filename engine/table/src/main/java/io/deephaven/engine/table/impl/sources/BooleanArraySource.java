/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.BooleanUtils;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.SoftRecycler;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.BooleanUtils.NULL_BOOLEAN_AS_BYTE;

/**
 * TODO: Re-implement this with better compression (only need 2 bits per value, or we can pack 5 values into 1 byte if we're feeling fancy).
 */
public class BooleanArraySource extends ArraySourceHelper<Boolean, byte[]> implements MutableColumnSourceGetDefaults.ForBoolean {
    private static final SoftRecycler<byte[]> recycler = new SoftRecycler<>(1024, () -> new byte[BLOCK_SIZE],
            null);
    private byte[][] blocks;
    transient private byte[][] prevBlocks;

    public BooleanArraySource() {
        super(Boolean.class);
        blocks = new byte[INITIAL_NUMBER_OF_BLOCKS][];
        maxIndex = INITIAL_MAX_INDEX;
    }

    @Override
    public void startTrackingPrevValues() {
        super.startTrackingPrev(blocks.length);
        prevBlocks = new byte[blocks.length][];
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        ensureCapacity(capacity, blocks, prevBlocks, nullFill);
    }

    @Override
    public void setNull(long key) {
        set(key, NULL_BOOLEAN_AS_BYTE);
    }

    @Override
    public void set(long key, Boolean value) {
        set(key, BooleanUtils.booleanAsByte(value));
    }

    @Override
    public void set(long key, byte value) {
        final int block = (int) (key >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (key & INDEX_MASK);
        if (shouldRecordPrevious(key, prevBlocks, recycler)) {
            prevBlocks[block][indexWithinBlock] = blocks[block][indexWithinBlock];
        }
        blocks[block][indexWithinBlock] = value;
    }

    @Override
    public Boolean get(long rowKey) {
        return BooleanUtils.byteAsBoolean(getByte(rowKey));
    }

    public Boolean getUnsafe(long index) {
        return BooleanUtils.byteAsBoolean(getByteUnsafe(index));
    }

    public final Boolean getAndSetUnsafe(long index, Boolean newValue) {
        return BooleanUtils.byteAsBoolean(getAndSetUnsafe(index, BooleanUtils.booleanAsByte(newValue)));
    }

    public final byte getAndSetUnsafe(long index, byte newValue) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        final byte oldValue = blocks[blockIndex][indexWithinBlock];
        // not a perfect comparison, but very cheap
        if (oldValue == newValue) {
            return oldValue;
        }
        if (shouldRecordPrevious(index, prevBlocks, recycler)) {
            prevBlocks[blockIndex][indexWithinBlock] = oldValue;
        }
        blocks[blockIndex][indexWithinBlock] = newValue;
        return oldValue;
    }

    @Override
    public byte getByte(long rowKey) {
        if (rowKey < 0 || rowKey > maxIndex) {
            return NULL_BOOLEAN_AS_BYTE;
        }
        return getByteUnsafe(rowKey);
    }

    private byte getByteUnsafe(long index) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        return blocks[blockIndex][indexWithinBlock];
    }

    @Override
    public byte getPrevByte(long rowKey) {
        if (rowKey < 0 || rowKey > maxIndex) {
            return NULL_BOOLEAN_AS_BYTE;
        }
        final int blockIndex = (int) (rowKey >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (rowKey & INDEX_MASK);
        if (shouldUsePrevious(rowKey)) {
            return prevBlocks[blockIndex][indexWithinBlock];
        } else {
            return blocks[blockIndex][indexWithinBlock];
        }
    }

    @Override
    public Boolean getPrev(long rowKey) {
        return BooleanUtils.byteAsBoolean(getPrevByte(rowKey));
    }

    @Override
    public void shift(long start, long end, long offset) {
        if (offset > 0) {
            for (long i = (int) end; i >= start; i--) {
                set((i + offset), getByte(i));
            }
        } else {
            for (int i = (int) start; i <= end; i++) {
                set((i + offset), getByte(i));
            }
        }
    }

    @Override
    byte[] allocateNullFilledBlock(int size) {
        final byte[] result = new byte[size];
        Arrays.fill(result, NULL_BOOLEAN_AS_BYTE);
        return result;
    }

    @Override
    final byte[] allocateBlock(int size) {
        return new byte[size];
    }

    @Override
    void resetBlocks(byte[][] newBlocks, byte[][] newPrev) {
        blocks = newBlocks;
        prevBlocks = newPrev;
    }

    @Override
    byte[][] getPrevBlocks() {
        return prevBlocks;
    }

    @Override
    SoftRecycler<byte[]> getRecycler() {
        return recycler;
    }

    @Override
    Object getBlock(int blockIndex) {
        return blocks[blockIndex];
    }

    @Override
    Object getPrevBlock(int blockIndex) {
        return blocks[blockIndex];
    }

    @Override
    protected void fillSparseChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {
        if (indices.size() == 0) {
            destGeneric.setSize(0);
            return;
        }
        final WritableObjectChunk<Boolean, ? super Values> dest = destGeneric.asWritableObjectChunk();
        final FillSparseChunkContext<byte[]> ctx = new FillSparseChunkContext<>();
        indices.forAllRowKeys((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1L) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
            }
            dest.set(ctx.offset++, BooleanUtils.byteAsBoolean(ctx.currentBlock[(int) (v & INDEX_MASK)]));
        });
        dest.setSize(ctx.offset);
    }

    @Override
    protected void fillSparsePrevChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {
        final long sz = indices.size();
        if (sz == 0) {
            destGeneric.setSize(0);
            return;
        }

        if (prevFlusher == null) {
            fillSparseChunk(destGeneric, indices);
            return;
        }

        final WritableObjectChunk<Boolean, ? super Values> dest = destGeneric.asWritableObjectChunk();
        final FillSparseChunkContext<byte[]> ctx = new FillSparseChunkContext<>();
        indices.forAllRowKeys((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1L) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
                ctx.currentPrevBlock = prevBlocks[ctx.currentBlockNo];
                ctx.prevInUseBlock = prevInUse[ctx.currentBlockNo];
            }

            final int indexWithinBlock = (int) (v & INDEX_MASK);
            final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
            final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);
            final boolean usePrev = ctx.prevInUseBlock != null && (ctx.prevInUseBlock[indexWithinInUse] & maskWithinInUse) != 0;
            final byte currValue = usePrev ? ctx.currentPrevBlock[indexWithinBlock] : ctx.currentBlock[indexWithinBlock];
            dest.set(ctx.offset++, BooleanUtils.byteAsBoolean(currValue));
        });
        dest.setSize(ctx.offset);
    }

    @Override
    protected void fillSparseChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
        final WritableObjectChunk<Boolean, ? super Values> dest = destGeneric.asWritableObjectChunk();
        final int sz = indices.size();
        for (int ii = 0; ii < sz; ++ii) {
            final long fromIndex = indices.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                dest.set(ii, null);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo >= blocks.length) {
                dest.set(ii, null);
            } else {
                final byte [] currentBlock = blocks[blockNo];
                dest.set(ii, BooleanUtils.byteAsBoolean(currentBlock[(int) (fromIndex & INDEX_MASK)]));
            }
        }
        dest.setSize(sz);
    }

    @Override
    protected void fillSparsePrevChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
        final WritableObjectChunk<Boolean, ? super Values> dest = destGeneric.asWritableObjectChunk();
        final int sz = indices.size();
        for (int ii = 0; ii < sz; ++ii) {
            final long fromIndex = indices.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                dest.set(ii, null);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo >= blocks.length) {
                dest.set(ii, null);
                continue;
            }
            final byte [] currentBlock = shouldUsePrevious(fromIndex) ? prevBlocks[blockNo] : blocks[blockNo];
            dest.set(ii, BooleanUtils.byteAsBoolean(currentBlock[(int) (fromIndex & INDEX_MASK)]));
        }
        dest.setSize(sz);
    }

    // the ArrayBackedColumnSource fillChunk can't handle our byte values as an Object
    @Override
    public void fillChunk(@NotNull final ColumnSource.FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        fillSparseChunk(destination, rowSequence);
    }

    @Override
    public boolean exposesChunkedBackingStore() {
        return false;
    }

    @Override
    public long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position) {
        // we can not support this operation, because the backing array is a byte and not the type of the column
        throw new UnsupportedOperationException();
    }

    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        // we can not support this operation, because the backing array is a byte and not the type of the column
        throw new UnsupportedOperationException();
    }

    @Override
    public void fillPrevChunk(@NotNull final ColumnSource.FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        fillSparsePrevChunk(destination, rowSequence);
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
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType.equals(byte.class);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        //noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) new ReinterpretedAsByte();
    }
    @Override
    void fillFromChunkByRanges(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        final ObjectChunk<Boolean, ? extends Values> chunk = src.asObjectChunk();
        fillFromChunkByRanges(rowSequence, src, offset -> BooleanUtils.booleanAsByte(chunk.get(offset)));
    }

    @Override
    void fillFromChunkByKeys(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        final ObjectChunk<Boolean, ? extends Values> chunk = src.asObjectChunk();
        fillFromChunkByKeys(rowSequence, src, offset -> BooleanUtils.booleanAsByte(chunk.get(offset)));
    }

    private interface Reader {
        byte getByte(int offset);
    }

    private void fillFromChunkByRanges(@NotNull RowSequence rowSequence, Chunk<? extends Values> src, Reader reader) {
        final LongChunk<OrderedRowKeyRanges> ranges = rowSequence.asRowKeyRangesChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        int offset = 0;
        // This helps us reduce the number of calls to Chunk.isAlias
        byte[] knownUnaliasedBlock = null;
        for (int ii = 0; ii < ranges.size(); ii += 2) {
            long firstKey = ranges.get(ii);
            final long lastKey = ranges.get(ii + 1);

            while (firstKey <= lastKey) {
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
                final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
                final int length = (int) (lastKeyToUse - firstKey + 1);

                final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
                final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
                final byte[] inner = blocks[block];

                if (inner != knownUnaliasedBlock && src.isAlias(inner)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
                knownUnaliasedBlock = inner;

                if (hasPrev) {
                    // this should be vectorized
                    for (int jj = 0; jj < length; ++jj) {
                        if (shouldRecordPrevious(firstKey + jj, prevBlocks, recycler)) {
                            prevBlocks[block][sIndexWithinBlock + jj] = inner[sIndexWithinBlock + jj];
                        }
                    }
                }

                // region copyToTypedArray
                for (int jj = 0; jj < length; ++jj) {
                    inner[sIndexWithinBlock + jj] = reader.getByte(offset + jj);
                }
                // endregion copyToTypedArray
                firstKey += length;
                offset += length;
            }
        }
    }

    private void fillFromChunkByKeys(@NotNull RowSequence rowSequence, Chunk<? extends Values> src, Reader reader) {
        final LongChunk<OrderedRowKeys> keys = rowSequence.asRowKeyChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
            int lastII = ii;
            while (lastII + 1 < keys.size() && keys.get(lastII + 1) <= maxKeyInCurrentBlock) {
                ++lastII;
            }

            final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
            final byte[] inner = blocks[block];

            if (src.isAlias(inner)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            while (ii <= lastII) {
                final long key = keys.get(ii);
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (hasPrev) {
                    if (shouldRecordPrevious(key, prevBlocks, recycler)) {
                        prevBlocks[block][indexWithinBlock] = inner[indexWithinBlock];
                    }
                }
                inner[indexWithinBlock] = reader.getByte(ii);
                ++ii;
            }
        }
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        final ObjectChunk<Boolean, ? extends Values> chunk = src.asObjectChunk();
        fillFromChunkUnordered(src, keys, offset -> BooleanUtils.booleanAsByte(chunk.get(offset)));
    }

    private void fillFromChunkUnordered(@NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys, Reader reader) {
        if (keys.size() == 0) {
            return;
        }
        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long minKeyInCurrentBlock = firstKey & ~INDEX_MASK;
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

            final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
            final byte [] inner = blocks[block];

            if (src.isAlias(inner)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            long key = keys.get(ii);
            do {
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (hasPrev) {
                    if (shouldRecordPrevious(key, prevBlocks, recycler)) {
                        prevBlocks[block][indexWithinBlock] = inner[indexWithinBlock];
                    }
                }
                inner[indexWithinBlock] = reader.getByte(ii);
                ++ii;
            } while (ii < keys.size() && (key = keys.get(ii)) >= minKeyInCurrentBlock && key <= maxKeyInCurrentBlock);
        }
    }

    private class ReinterpretedAsByte extends AbstractColumnSource<Byte> implements MutableColumnSourceGetDefaults.ForByte, FillUnordered<Values>, WritableColumnSource<Byte> {
        private ReinterpretedAsByte() {
            super(byte.class);
        }

        @Override
        public void startTrackingPrevValues() {
            BooleanArraySource.this.startTrackingPrevValues();
        }

        @Override
        public byte getByte(long rowKey) {
            return BooleanArraySource.this.getByte(rowKey);
        }

        @Override
        public byte getPrevByte(long rowKey) {
            return BooleanArraySource.this.getPrevByte(rowKey);
        }

        @Override
        public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return alternateDataType == Boolean.class;
        }

        @Override
        protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            //noinspection unchecked
            return (ColumnSource<ALTERNATE_DATA_TYPE>) BooleanArraySource.this;
        }

        protected void fillSparseChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {
            if (indices.size() == 0) {
                destGeneric.setSize(0);
                return;
            }
            final WritableByteChunk<? super Values> dest = destGeneric.asWritableByteChunk();
            final FillSparseChunkContext<byte[]> ctx = new FillSparseChunkContext<>();
            indices.forAllRowKeys((final long v) -> {
                if (v >= ctx.capForCurrentBlock) {
                    ctx.currentBlockNo = getBlockNo(v);
                    ctx.capForCurrentBlock = (ctx.currentBlockNo + 1L) << LOG_BLOCK_SIZE;
                    ctx.currentBlock = blocks[ctx.currentBlockNo];
                }
                dest.set(ctx.offset++, ctx.currentBlock[(int) (v & INDEX_MASK)]);
            });
            dest.setSize(ctx.offset);
        }

        protected void fillSparsePrevChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {
            final long sz = indices.size();
            if (sz == 0) {
                destGeneric.setSize(0);
                return;
            }

            if (prevFlusher == null) {
                fillSparseChunk(destGeneric, indices);
                return;
            }

            final WritableByteChunk<? super Values> dest = destGeneric.asWritableByteChunk();
            final FillSparseChunkContext<byte[]> ctx = new FillSparseChunkContext<>();
            indices.forAllRowKeys((final long v) -> {
                if (v >= ctx.capForCurrentBlock) {
                    ctx.currentBlockNo = getBlockNo(v);
                    ctx.capForCurrentBlock = (ctx.currentBlockNo + 1L) << LOG_BLOCK_SIZE;
                    ctx.currentBlock = blocks[ctx.currentBlockNo];
                    ctx.currentPrevBlock = prevBlocks[ctx.currentBlockNo];
                    ctx.prevInUseBlock = prevInUse[ctx.currentBlockNo];
                }

                final int indexWithinBlock = (int) (v & INDEX_MASK);
                final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);
                final boolean usePrev = ctx.prevInUseBlock != null && (ctx.prevInUseBlock[indexWithinInUse] & maskWithinInUse) != 0;
                final byte currValue = usePrev ? ctx.currentPrevBlock[indexWithinBlock] : ctx.currentBlock[indexWithinBlock];
                dest.set(ctx.offset++, currValue);
            });
            dest.setSize(ctx.offset);
        }

        protected void fillSparseChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
            final WritableByteChunk<? super Values> dest = destGeneric.asWritableByteChunk();
            final int sz = indices.size();
            for (int ii = 0; ii < sz; ++ii) {
                final long fromIndex = indices.get(ii);
                if (fromIndex == RowSequence.NULL_ROW_KEY) {
                    dest.set(ii, NULL_BOOLEAN_AS_BYTE);
                    continue;
                }
                final int blockNo = getBlockNo(fromIndex);
                if (blockNo >= blocks.length) {
                    dest.set(ii, NULL_BOOLEAN_AS_BYTE);
                } else {
                    final byte [] currentBlock = blocks[blockNo];
                    dest.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
                }
            }
            dest.setSize(sz);
        }

        protected void fillSparsePrevChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
            final WritableByteChunk<? super Values> dest = destGeneric.asWritableByteChunk();
            final int sz = indices.size();
            for (int ii = 0; ii < sz; ++ii) {
                final long fromIndex = indices.get(ii);
                if (fromIndex == RowSequence.NULL_ROW_KEY) {
                    dest.set(ii, NULL_BOOLEAN_AS_BYTE);
                    continue;
                }
                final int blockNo = getBlockNo(fromIndex);
                if (blockNo >= blocks.length) {
                    dest.set(ii, NULL_BOOLEAN_AS_BYTE);
                    continue;
                }
                final byte [] currentBlock = shouldUsePrevious(fromIndex) ? prevBlocks[blockNo] : blocks[blockNo];
                dest.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
            }
            dest.setSize(sz);
        }

        @Override
        public void fillChunk(@NotNull final ColumnSource.FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
            fillSparseChunk(destination, rowSequence);
        }

        @Override
        public void fillPrevChunk(@NotNull final ColumnSource.FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
            fillSparsePrevChunk(destination, rowSequence);
        }

        @Override
        public void fillChunkUnordered(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final LongChunk<? extends RowKeys> keyIndices) {
            fillSparseChunkUnordered(destination, keyIndices);
        }

        @Override
        public void fillPrevChunkUnordered(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final LongChunk<? extends RowKeys> keyIndices) {
            fillSparsePrevChunkUnordered(destination, keyIndices);
        }

        @Override
        public void setNull(long key) {
            BooleanArraySource.this.setNull(key);
        }

        @Override
        public void set(long key, byte value) {
            BooleanArraySource.this.set(key, value);
        }

        @Override
        public void ensureCapacity(long capacity, boolean nullFill) {
            BooleanArraySource.this.ensureCapacity(capacity, nullFill);
        }

        @Override
        public FillFromContext makeFillFromContext(int chunkCapacity) {
            return BooleanArraySource.this.makeFillFromContext(chunkCapacity);
        }

        @Override
        public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
                                  @NotNull RowSequence rowSequence) {
            final ByteChunk<? extends Values> chunk = src.asByteChunk();
            if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
                fillFromChunkByKeys(rowSequence, src, chunk::get);
            } else {
                fillFromChunkByRanges(rowSequence, src, chunk::get);
            }
        }

        @Override
        public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
            final ByteChunk<? extends Values> chunk = src.asByteChunk();
            BooleanArraySource.this.fillFromChunkUnordered(src, keys, chunk::get);
        }

        @Override
        public boolean providesFillUnordered() {
            return true;
        }
    }
}
