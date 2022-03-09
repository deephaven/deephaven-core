/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.util.SoftRecycler;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Shared implementation for DateTimeArraySource and LongArraySource (ArraySources that have 'long' as their underlying
 * element type).
 */
public abstract class AbstractLongArraySource<T> extends ArraySourceHelper<T, long[]> implements MutableColumnSourceGetDefaults.LongBacked<T> {
    private static SoftRecycler<long[]> recycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new long[BLOCK_SIZE], null);
    private long[][] blocks;
    private transient long[][] prevBlocks;

    AbstractLongArraySource(Class<T> type) {
        super(type);
        blocks = new long[INITIAL_NUMBER_OF_BLOCKS][];
        maxIndex = INITIAL_MAX_INDEX;
    }

    @Override
    public void startTrackingPrevValues() {
        super.startTrackingPrev(blocks.length);
        prevBlocks = new long[blocks.length][];
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        ensureCapacity(capacity, blocks, prevBlocks, nullFill);
    }

    @Override
    public final void set(long key, long value) {
        final int block = (int) (key >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (key & INDEX_MASK);
        if (shouldRecordPrevious(key, prevBlocks, recycler)) {
            prevBlocks[block][indexWithinBlock] = blocks[block][indexWithinBlock];
        }
        blocks[block][indexWithinBlock] = value;
    }

    @Override
    public final long getLong(long index) {
        if (index < 0 || index > maxIndex) {
            return NULL_LONG;
        }
        return getUnsafe(index);
    }

    public final long getUnsafe(long index) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        return blocks[blockIndex][indexWithinBlock];
    }

    public final long getAndSetUnsafe(long index, long newValue) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        final long oldValue = blocks[blockIndex][indexWithinBlock];
        if (oldValue != newValue) {
            if (shouldRecordPrevious(index, prevBlocks, recycler)) {
                prevBlocks[blockIndex][indexWithinBlock] = oldValue;
            }
            blocks[blockIndex][indexWithinBlock] = newValue;
        }
        return oldValue;
    }

    public final long getAndAddUnsafe(long index, long addend) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        final long oldValue = blocks[blockIndex][indexWithinBlock];
        if (addend != 0) {
            if (shouldRecordPrevious(index, prevBlocks, recycler)) {
                prevBlocks[blockIndex][indexWithinBlock] = oldValue;
            }
            blocks[blockIndex][indexWithinBlock] = oldValue + addend;
        }
        return oldValue;
    }

    @Override
    public final long getPrevLong(long index) {
        if (index < 0 || index > maxIndex) {
            return NULL_LONG;
        }
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        if (shouldUsePrevious(index)) {
            return prevBlocks[blockIndex][indexWithinBlock];
        } else {
            return blocks[blockIndex][indexWithinBlock];
        }
    }

    @Override
    public void shift(long start, long end, long offset) {
        if (offset > 0) {
            for (long i = (int) end; i >= start; i--) {
                set((i + offset), getLong(i));
            }
        } else {
            for (int i = (int) start; i <= end; i++) {
                set((i + offset), getLong(i));
            }
        }
    }

    public void move(long source, long dest, long length) {
        if (prevBlocks != null) {
            throw new UnsupportedOperationException();
        }
        if (source == dest) {
            return;
        }
        if (((source - dest) & INDEX_MASK) == 0 && (source & INDEX_MASK) == 0) {
            // TODO: we can move full blocks!
        }
        if (source < dest) {
            for (long ii = length - 1; ii >= 0; ) {
                final long sourceKey = source + ii;
                final long destKey = dest + ii;
                final int sourceBlock = (int) (sourceKey >> LOG_BLOCK_SIZE);
                final int sourceIndexWithinBlock = (int) (sourceKey & INDEX_MASK);

                final int destBlock = (int) (destKey >> LOG_BLOCK_SIZE);
                final int destIndexWithinBlock = (int) (destKey & INDEX_MASK);

                final int valuesInBothBlocks = Math.min(destIndexWithinBlock + 1, sourceIndexWithinBlock + 1);
                final int toMove = (ii + 1) < valuesInBothBlocks ? (int)(ii + 1): valuesInBothBlocks;

                System.arraycopy(blocks[sourceBlock], sourceIndexWithinBlock - toMove + 1, blocks[destBlock], destIndexWithinBlock - toMove + 1, toMove);
                ii -= toMove;
            }
        } else {
            for (long ii = 0; ii < length;) {
                final long sourceKey = source + ii;
                final long destKey = dest + ii;
                final int sourceBlock = (int) (sourceKey >> LOG_BLOCK_SIZE);
                final int sourceIndexWithinBlock = (int) (sourceKey & INDEX_MASK);

                final int destBlock = (int) (destKey >> LOG_BLOCK_SIZE);
                final int destIndexWithinBlock = (int) (destKey & INDEX_MASK);

                final int valuesInBothBlocks = BLOCK_SIZE - Math.max(destIndexWithinBlock, sourceIndexWithinBlock);
                final int toMove = (BLOCK_SIZE - ii) < valuesInBothBlocks ? (int)(BLOCK_SIZE - ii): valuesInBothBlocks;

                System.arraycopy(blocks[sourceBlock], sourceIndexWithinBlock, blocks[destBlock], destIndexWithinBlock, toMove);
                ii += toMove;
            }
        }
    }

    @Override
    long[] allocateNullFilledBlock(int size) {
        final long[] newBlock = new long[size];
        Arrays.fill(newBlock, NULL_LONG);
        return newBlock;
    }

    @Override
    final long[] allocateBlock(int size) {
        return new long[size];
    }

    @Override
    void resetBlocks(long[][] newBlocks, long[][] newPrev) {
        blocks = newBlocks;
        prevBlocks = newPrev;
    }

    @Override
    long[][] getPrevBlocks() {
        return prevBlocks;
    }

    @Override
    SoftRecycler<long[]> getRecycler() {
        return recycler;
    }

    @Override
    Object getBlock(int blockIndex) {
        return blocks[blockIndex];
    }

    @Override
    Object getPrevBlock(int blockIndex) {
        return prevBlocks[blockIndex];
    }

    @Override
    protected void fillSparseChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {
        fillSparseLongChunk(destGeneric, indices);
    }

    protected final void fillSparseLongChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {
        final long sz = indices.size();
        if (sz == 0) {
            destGeneric.setSize(0);
            return;
        }
        final WritableLongChunk<? super Values> dest = destGeneric.asWritableLongChunk();
        final FillSparseChunkContext<long[]> ctx = new FillSparseChunkContext<>();
        indices.forAllRowKeys((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
            }
            dest.set(ctx.offset++, ctx.currentBlock[(int) (v & INDEX_MASK)]);
        });
        dest.setSize(ctx.offset);
    }

    @Override
    protected void fillSparsePrevChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {
        fillSparsePrevLongChunk(destGeneric, indices);
    }

    protected final void fillSparsePrevLongChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {
        final long sz = indices.size();
        if (sz == 0) {
            destGeneric.setSize(0);
            return;
        }

        if (prevFlusher == null) {
            fillSparseLongChunk(destGeneric, indices);
            return;
        }

        final WritableLongChunk<? super Values> dest = destGeneric.asWritableLongChunk();
        final FillSparseChunkContext<long[]> ctx = new FillSparseChunkContext<>();
        indices.forAllRowKeys((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
                ctx.currentPrevBlock = prevBlocks[ctx.currentBlockNo];
                ctx.prevInUseBlock = prevInUse[ctx.currentBlockNo];
            }

            final int indexWithinBlock = (int) (v & INDEX_MASK);
            final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
            final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);
            final boolean usePrev = ctx.prevInUseBlock != null && (ctx.prevInUseBlock[indexWithinInUse] & maskWithinInUse) != 0;
            dest.set(ctx.offset++, usePrev ? ctx.currentPrevBlock[indexWithinBlock] : ctx.currentBlock[indexWithinBlock]);
        });
        dest.setSize(ctx.offset);
    }

    @Override
    protected void fillSparseChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
        fillSparseLongChunkUnordered(destGeneric, indices);
    }

    protected final void fillSparseLongChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
        final WritableLongChunk<? super Values> dest = destGeneric.asWritableLongChunk();
        final int sz = indices.size();
        for (int ii = 0; ii < sz; ii++) {
            final long fromIndex = indices.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                dest.set(ii, NULL_LONG);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo > blocks.length) {
                dest.set(ii, NULL_LONG);
                continue;
            }
            final long[] currentBlock = blocks[blockNo];
            dest.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
        }
        dest.setSize(sz);
    }

    @Override
    protected void fillSparsePrevChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
        fillSparsePrevLongChunkUnordered(destGeneric, indices);
    }

    protected final void fillSparsePrevLongChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
        final WritableLongChunk<? super Values> dest = destGeneric.asWritableLongChunk();
        final int sz = indices.size();
        for (int ii = 0; ii < sz; ii++) {
            final long fromIndex = indices.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                dest.set(ii, NULL_LONG);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo > blocks.length) {
                dest.set(ii, NULL_LONG);
                continue;
            }
            final long[] currentBlock = shouldUsePrevious(fromIndex) ? prevBlocks[blockNo] : blocks[blockNo];
            dest.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
        }
        dest.setSize(sz);
    }

    protected <R> void fillSparseChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices, final LongFunction<R> mapper) {
        final long sz = indices.size();
        if (sz == 0) {
            destGeneric.setSize(0);
            return;
        }
        final WritableObjectChunk<R, ? super Values> dest = destGeneric.asWritableObjectChunk();
        final FillSparseChunkContext<long[]> ctx = new FillSparseChunkContext<>();
        indices.forAllRowKeys((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
            }
            dest.set(ctx.offset++, mapper.apply(ctx.currentBlock[(int) (v & INDEX_MASK)]));
        });
        dest.setSize(ctx.offset);
    }

    protected <R> void fillSparsePrevChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices, final LongFunction<R> mapper) {
        final long sz = indices.size();
        if (sz == 0) {
            destGeneric.setSize(0);
            return;
        }

        if (prevFlusher == null) {
            fillSparseChunk(destGeneric, indices, mapper);
            return;
        }

        final WritableObjectChunk<R, ? super Values> dest = destGeneric.asWritableObjectChunk();
        final FillSparseChunkContext<long[]> ctx = new FillSparseChunkContext<>();
        indices.forAllRowKeys((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
                ctx.currentPrevBlock = prevBlocks[ctx.currentBlockNo];
                ctx.prevInUseBlock = prevInUse[ctx.currentBlockNo];
            }

            final int indexWithinBlock = (int) (v & INDEX_MASK);
            final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
            final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);
            final boolean usePrev = ctx.prevInUseBlock != null && (ctx.prevInUseBlock[indexWithinInUse] & maskWithinInUse) != 0;
            final long currValue = usePrev ? ctx.currentPrevBlock[indexWithinBlock] : ctx.currentBlock[indexWithinBlock];
            dest.set(ctx.offset++, mapper.apply(currValue));
        });
        dest.setSize(ctx.offset);
    }

    protected <R> void fillSparseChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices, final LongFunction<R> mapper) {
        final WritableObjectChunk<R, ? super Values> dest = destGeneric.asWritableObjectChunk();
        final int sz = indices.size();
        final R nullValue = mapper.apply(NULL_LONG);
        for (int ii = 0; ii < sz; ii++) {
            final long fromIndex = indices.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                dest.set(ii, nullValue);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo > blocks.length) {
                dest.set(ii, nullValue);
                continue;
            }
            final long[] currentBlock = blocks[blockNo];
            dest.set(ii, mapper.apply(currentBlock[(int) (fromIndex & INDEX_MASK)]));
        }
        dest.setSize(sz);
    }

    protected <R> void fillSparsePrevChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices, final LongFunction<R> mapper) {
        final WritableObjectChunk<R, ? super Values> dest = destGeneric.asWritableObjectChunk();
        final int sz = indices.size();
        final R nullValue = mapper.apply(NULL_LONG);
        for (int ii = 0; ii < sz; ii++) {
            final long fromIndex = indices.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                dest.set(ii, nullValue);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo > blocks.length) {
                dest.set(ii, nullValue);
                continue;
            }
            final long[] currentBlock = shouldUsePrevious(fromIndex) ? prevBlocks[blockNo] : blocks[blockNo];
            dest.set(ii, mapper.apply(currentBlock[(int) (fromIndex & INDEX_MASK)]));
        }
        dest.setSize(sz);
    }

    @Override
    public long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk chunk, long position) {
        Assert.eqNull(prevInUse, "prevInUse");
        final int blockNo = getBlockNo(position);
        final long [] backingArray = blocks[blockNo];
        chunk.asResettableWritableLongChunk().resetFromTypedArray(backingArray, 0, BLOCK_SIZE);
        return blockNo << LOG_BLOCK_SIZE;
    }

    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        Assert.eqNull(prevInUse, "prevInUse");
        final int blockNo = getBlockNo(position);
        final long [] backingArray = blocks[blockNo];
        final long firstPosition = ((long) blockNo) << LOG_BLOCK_SIZE;
        final int offset = (int)(position - firstPosition);
        final int capacity = BLOCK_SIZE - offset;
        chunk.asResettableWritableLongChunk().resetFromTypedArray(backingArray, offset, capacity);
        return capacity;
    }

    @Override
    void fillFromChunkByRanges(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        final LongChunk<? extends Values> chunk = src.asLongChunk();
        final LongChunk<OrderedRowKeyRanges> ranges = rowSequence.asRowKeyRangesChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        int offset = 0;
        // This helps us reduce the number of calls to Chunk.isAlias
        long[] knownUnaliasedBlock = null;
        for (int ii = 0; ii < ranges.size(); ii += 2) {
            long firstKey = ranges.get(ii);
            final long lastKey = ranges.get(ii + 1);

            while (firstKey <= lastKey) {
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
                final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
                final int length = (int) (lastKeyToUse - firstKey + 1);

                final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
                final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
                final long[] inner = blocks[block];

                if (inner != knownUnaliasedBlock && chunk.isAlias(inner)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
                knownUnaliasedBlock = inner;

                // This 'if' with its constant condition should be very friendly to the branch predictor.
                if (hasPrev) {
                    // this should be vectorized
                    for (int jj = 0; jj < length; ++jj) {
                        if (shouldRecordPrevious(firstKey + jj, prevBlocks, recycler)) {
                            prevBlocks[block][sIndexWithinBlock + jj] = inner[sIndexWithinBlock + jj];
                        }
                    }
                }

                chunk.copyToTypedArray(offset, inner, sIndexWithinBlock, length);
                firstKey += length;
                offset += length;
            }
        }
    }

    public void copyFromChunk(long firstKey, long totalLength, Chunk<? extends Values> src, int offset) {
        if (totalLength == 0) {
            return;
        }
        final LongChunk<? extends Values> chunk = src.asLongChunk();

        final long lastKey = firstKey + totalLength - 1;

        while (firstKey <= lastKey) {
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
            final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
            final int length = (int) (lastKeyToUse - firstKey + 1);

            final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
            final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
            final long[] inner = blocks[block];

            chunk.copyToTypedArray(offset, inner, sIndexWithinBlock, length);
            firstKey += length;
            offset += length;
        }
    }

    @Override
    void fillFromChunkByKeys(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        final LongChunk<? extends Values> chunk = src.asLongChunk();
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
            final long[] inner = blocks[block];

            if (chunk.isAlias(inner)) {
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
                inner[indexWithinBlock] = chunk.get(ii);
                ++ii;
            }
        }
    }

    <T> void fillFromChunkByRanges(final @NotNull RowSequence rowSequence, final Chunk<? extends Values> src, final ToLongFunction<T> mapper) {
        final ObjectChunk<T, ? extends Values> chunk = src.asObjectChunk();
        final LongChunk<OrderedRowKeyRanges> ranges = rowSequence.asRowKeyRangesChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        int offset = 0;
        // This helps us reduce the number of calls to Chunk.isAlias
        long[] knownUnaliasedBlock = null;
        for (int ii = 0; ii < ranges.size(); ii += 2) {
            long firstKey = ranges.get(ii);
            final long lastKey = ranges.get(ii + 1);

            while (firstKey <= lastKey) {
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
                final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
                final int length = (int) (lastKeyToUse - firstKey + 1);

                final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
                final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
                final long[] inner = blocks[block];

                if (inner != knownUnaliasedBlock && chunk.isAlias(inner)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
                knownUnaliasedBlock = inner;

                // This 'if' with its constant condition should be very friendly to the branch predictor.
                if (hasPrev) {
                    // this should be vectorized
                    for (int jj = 0; jj < length; ++jj) {
                        if (shouldRecordPrevious(firstKey + jj, prevBlocks, recycler)) {
                            prevBlocks[block][sIndexWithinBlock + jj] = inner[sIndexWithinBlock + jj];
                        }
                    }
                }

                for (int jj = 0; jj < length; ++jj) {
                    inner[sIndexWithinBlock + jj] = mapper.applyAsLong(chunk.get(offset + jj));
                }
                firstKey += length;
                offset += length;
            }
        }
    }

    <T> void fillFromChunkByKeys(final @NotNull RowSequence rowSequence, final Chunk<? extends Values> src, final ToLongFunction<T> mapper) {
        final ObjectChunk<T, ? extends Values> chunk = src.asObjectChunk();
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
            final long[] inner = blocks[block];

            if (chunk.isAlias(inner)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            while (ii <= lastII) {
                final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);

                if (hasPrev) {
                    if (shouldRecordPrevious(ii, prevBlocks, recycler)) {
                        prevBlocks[block][indexWithinBlock] = inner[indexWithinBlock];
                    }
                }
                inner[indexWithinBlock] = mapper.applyAsLong(chunk.get(ii));
                ++ii;
            }
        }
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        if (keys.size() == 0) {
            return;
        }
        final LongChunk<? extends Values> chunk = src.asLongChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long minKeyInCurrentBlock = firstKey & ~INDEX_MASK;
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

            final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
            final long[] inner = blocks[block];

            if (chunk.isAlias(inner)) {
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
                inner[indexWithinBlock] = chunk.get(ii);
                ++ii;
            } while (ii < keys.size() && (key = keys.get(ii)) >= minKeyInCurrentBlock && key <= maxKeyInCurrentBlock);
        }
    }

    <T> void fillFromChunkUnordered(@NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys, final ToLongFunction<T> mapper) {
        if (keys.size() == 0) {
            return;
        }
        final ObjectChunk<T, ? extends Values> chunk = src.asObjectChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long minKeyInCurrentBlock = firstKey & ~INDEX_MASK;
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

            final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
            final long[] inner = blocks[block];

            if (chunk.isAlias(inner)) {
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
                inner[indexWithinBlock] = mapper.applyAsLong(chunk.get(ii));
                ++ii;
            } while (ii < keys.size() && (key = keys.get(ii)) >= minKeyInCurrentBlock && key <= maxKeyInCurrentBlock);
        }
    }
}
