/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.util.DhShortComparisons;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ResettableWritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableShortChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.SoftRecycler;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static io.deephaven.util.type.TypeUtils.box;
import static io.deephaven.util.type.TypeUtils.unbox;
import static io.deephaven.db.v2.sources.chunk.Attributes.*;

/**
 * Simple array source for Short.
 * <p>
 * The C-haracterArraySource is replicated to all other types with
 * io.deephaven.db.v2.sources.Replicate.
 *
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ShortArraySource extends ArraySourceHelper<Short, short[]> implements MutableColumnSourceGetDefaults.ForShort {
    private static final SoftRecycler<short[]> recycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new short[BLOCK_SIZE], null);

    private short[][] blocks;
    private transient short[][] prevBlocks;

    public ShortArraySource() {
        super(short.class);
        blocks = new short[INITIAL_NUMBER_OF_BLOCKS][];
        maxIndex = INITIAL_MAX_INDEX;
    }

    @Override
    public void startTrackingPrevValues() {
        super.startTrackingPrev(blocks.length);
        prevBlocks = new short[blocks.length][];
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        ensureCapacity(capacity, blocks, prevBlocks, nullFill);
    }

    @Override
    public final void set(long key, Short value) {
        set(key, unbox(value));
    }

    @Override
    public final void set(long key, short value) {
        final int block = (int) (key >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (key & INDEX_MASK);
        if (shouldRecordPrevious(key, prevBlocks, recycler)) {
            prevBlocks[block][indexWithinBlock] = blocks[block][indexWithinBlock];
        }
        blocks[block][indexWithinBlock] = value;
    }

    @Override
    public Short get(long index) {
        if (index < 0 || index > maxIndex) {
            return null;
        }
        return box(blocks[((int) (index >> LOG_BLOCK_SIZE))][((int) (index & INDEX_MASK))]);
    }

    @Override
    public final short getShort(long index) {
        if (index < 0 || index > maxIndex) {
            return NULL_SHORT;
        }
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        return blocks[blockIndex][indexWithinBlock];
    }

    public final short getUnsafe(long index) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        return blocks[blockIndex][indexWithinBlock];
    }

    public final short getAndSetUnsafe(long index, short newValue) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        final short oldValue = blocks[blockIndex][indexWithinBlock];
        if (!DhShortComparisons.eq(oldValue, newValue)) {
            if (shouldRecordPrevious(index, prevBlocks, recycler)) {
                prevBlocks[blockIndex][indexWithinBlock] = oldValue;
            }
            blocks[blockIndex][indexWithinBlock] = newValue;
        }
        return oldValue;
    }

    @Override
    public Short getPrev(long index) {
        return box(getPrevShort(index));
    }

    @Override
    public final short getPrevShort(long index) {
        if (index < 0 || index > maxIndex) {
            return NULL_SHORT;
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
    public void copy(ColumnSource<? extends Short> sourceColumn, long sourceKey, long destKey) {
        set(destKey, sourceColumn.getShort(sourceKey));
    }

    @Override
    public void shift(long start, long end, long offset) {
        if (offset > 0) {
            for (long i = (int) end; i >= start; i--) {
                set((i + offset), getShort(i));
            }
        } else {
            for (int i = (int) start; i <= end; i++) {
                set((i + offset), getShort(i));
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
        if (source < dest && source + length >= dest) {
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
                final int toMove = (length - ii < valuesInBothBlocks) ? (int)(length - ii): valuesInBothBlocks;

                System.arraycopy(blocks[sourceBlock], sourceIndexWithinBlock, blocks[destBlock], destIndexWithinBlock, toMove);
                ii += toMove;
            }
        }
    }

    @Override
    final short[] allocateNullFilledBlock(int size) {
        final short[] newBlock = new short[size];
        Arrays.fill(newBlock, NULL_SHORT);
        return newBlock;
    }

    @Override
    final short[] allocateBlock(int size) {
        return new short[size];
    }

    @Override
    void resetBlocks(short[][] newBlocks, short[][] newPrev) {
        blocks = newBlocks;
        prevBlocks = newPrev;
    }

    @Override
    short[][] getPrevBlocks() {
        return prevBlocks;
    }

    @Override
    SoftRecycler<short[]> getRecycler() {
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
    public long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position) {
        Assert.eqNull(prevInUse, "prevInUse");
        final int blockNo = getBlockNo(position);
        final short [] backingArray = blocks[blockNo];
        chunk.asResettableWritableShortChunk().resetFromTypedArray(backingArray, 0, BLOCK_SIZE);
        return ((long)blockNo) << LOG_BLOCK_SIZE;
    }

    @Override
    protected void fillSparseChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final OrderedKeys indices) {
        if (indices.size() == 0) {
            destGeneric.setSize(0);
            return;
        }
        final WritableShortChunk<? super Values> dest = destGeneric.asWritableShortChunk();
        final FillSparseChunkContext<short[]> ctx = new FillSparseChunkContext<>();
        indices.forAllLongs((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1L) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
            }
            dest.set(ctx.offset++, ctx.currentBlock[(int) (v & INDEX_MASK)]);
        });
        dest.setSize(ctx.offset);
    }

    @Override
    protected void fillSparsePrevChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final OrderedKeys indices) {
        final long sz = indices.size();
        if (sz == 0) {
            destGeneric.setSize(0);
            return;
        }

        if (prevFlusher == null) {
            fillSparseChunk(destGeneric, indices);
            return;
        }

        final WritableShortChunk<? super Values> dest = destGeneric.asWritableShortChunk();
        final FillSparseChunkContext<short[]> ctx = new FillSparseChunkContext<>();
        indices.forAllLongs((final long v) -> {
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
            dest.set(ctx.offset++, usePrev ? ctx.currentPrevBlock[indexWithinBlock] : ctx.currentBlock[indexWithinBlock]);
        });
        dest.setSize(ctx.offset);
    }

    @Override
    protected void fillSparseChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends KeyIndices> indices) {
        final WritableShortChunk<? super Values> dest = destGeneric.asWritableShortChunk();
        final int sz = indices.size();
        for (int ii = 0; ii < sz; ++ii) {
            final long fromIndex = indices.get(ii);
            if (fromIndex == Index.NULL_KEY) {
                dest.set(ii, NULL_SHORT);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo >= blocks.length) {
                dest.set(ii, NULL_SHORT);
            } else {
                final short[] currentBlock = blocks[blockNo];
                dest.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
            }
        }
        dest.setSize(sz);
    }

    @Override
    protected void fillSparsePrevChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends KeyIndices> indices) {
        final WritableShortChunk<? super Values> dest = destGeneric.asWritableShortChunk();
        final int sz = indices.size();
        for (int ii = 0; ii < sz; ++ii) {
            final long fromIndex = indices.get(ii);
            if (fromIndex == Index.NULL_KEY) {
                dest.set(ii, NULL_SHORT);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo >= blocks.length) {
                dest.set(ii, NULL_SHORT);
                continue;
            }
            final short[] currentBlock = shouldUsePrevious(fromIndex) ? prevBlocks[blockNo] : blocks[blockNo];
            dest.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
        }
        dest.setSize(sz);
    }

    @Override
    void fillFromChunkByRanges(@NotNull OrderedKeys orderedKeys, Chunk<? extends Values> src) {
        if (orderedKeys.size() == 0) {
            return;
        }
        final ShortChunk<? extends Values> chunk = src.asShortChunk();
        final LongChunk<OrderedKeyRanges> ranges = orderedKeys.asKeyRangesChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        int offset = 0;
        // This helps us reduce the number of calls to Chunk.isAlias
        short[] knownUnaliasedBlock = null;
        for (int ii = 0; ii < ranges.size(); ii += 2) {
            long firstKey = ranges.get(ii);
            final long lastKey = ranges.get(ii + 1);

            while (firstKey <= lastKey) {
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
                final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
                final int length = (int) (lastKeyToUse - firstKey + 1);

                final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
                final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
                final short[] inner = blocks[block];

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
        final ShortChunk<? extends Values> chunk = src.asShortChunk();

        final long lastKey = firstKey + totalLength - 1;

        while (firstKey <= lastKey) {
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
            final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
            final int length = (int) (lastKeyToUse - firstKey + 1);

            final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
            final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
            final short[] inner = blocks[block];

            chunk.copyToTypedArray(offset, inner, sIndexWithinBlock, length);
            firstKey += length;
            offset += length;
        }
    }

    @Override
    void fillFromChunkByKeys(@NotNull OrderedKeys orderedKeys, Chunk<? extends Values> src) {
        if (orderedKeys.size() == 0) {
            return;
        }
        final ShortChunk<? extends Values> chunk = src.asShortChunk();
        final LongChunk<OrderedKeyIndices> keys = orderedKeys.asKeyIndicesChunk();

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
            final short[] inner = blocks[block];

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

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<KeyIndices> keys) {
        if (keys.size() == 0) {
            return;
        }
        final ShortChunk<? extends Values> chunk = src.asShortChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long minKeyInCurrentBlock = firstKey & ~INDEX_MASK;
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

            final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
            final short[] inner = blocks[block];

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
}
