/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.chunk.*;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.SoftRecycler;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.type.TypeUtils.box;
import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Simple array source for Double.
 * <p>
 * The C-haracterArraySource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class DoubleArraySource extends ArraySourceHelper<Double, double[]> implements MutableColumnSourceGetDefaults.ForDouble {
    private static final SoftRecycler<double[]> recycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new double[BLOCK_SIZE], null);

    private double[][] blocks;
    private transient double[][] prevBlocks;

    public DoubleArraySource() {
        super(double.class);
        blocks = new double[INITIAL_NUMBER_OF_BLOCKS][];
        maxIndex = INITIAL_MAX_INDEX;
    }

    @Override
    public void startTrackingPrevValues() {
        super.startTrackingPrev(blocks.length);
        prevBlocks = new double[blocks.length][];
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        ensureCapacity(capacity, blocks, prevBlocks, nullFill);
    }

    @Override
    public final void set(long key, Double value) {
        set(key, unbox(value));
    }

    @Override
    public final void set(long key, double value) {
        final int block = (int) (key >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (key & INDEX_MASK);
        if (shouldRecordPrevious(key, prevBlocks, recycler)) {
            prevBlocks[block][indexWithinBlock] = blocks[block][indexWithinBlock];
        }
        blocks[block][indexWithinBlock] = value;
    }

    @Override
    public final double getDouble(long rowKey) {
        if (rowKey < 0 || rowKey > maxIndex) {
            return NULL_DOUBLE;
        }
        return getUnsafe(rowKey);
    }

    public final double getUnsafe(long index) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        return blocks[blockIndex][indexWithinBlock];
    }

    public final double getAndSetUnsafe(long index, double newValue) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        final double oldValue = blocks[blockIndex][indexWithinBlock];
        if (!DoubleComparisons.eq(oldValue, newValue)) {
            if (shouldRecordPrevious(index, prevBlocks, recycler)) {
                prevBlocks[blockIndex][indexWithinBlock] = oldValue;
            }
            blocks[blockIndex][indexWithinBlock] = newValue;
        }
        return oldValue;
    }

    @Override
    public Double getPrev(long rowKey) {
        return box(getPrevDouble(rowKey));
    }

    @Override
    public final double getPrevDouble(long rowKey) {
        if (rowKey < 0 || rowKey > maxIndex) {
            return NULL_DOUBLE;
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
    public void shift(long start, long end, long offset) {
        if (offset > 0) {
            for (long i = (int) end; i >= start; i--) {
                set((i + offset), getDouble(i));
            }
        } else {
            for (int i = (int) start; i <= end; i++) {
                set((i + offset), getDouble(i));
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
    final double[] allocateNullFilledBlock(int size) {
        final double[] newBlock = new double[size];
        Arrays.fill(newBlock, NULL_DOUBLE);
        return newBlock;
    }

    @Override
    final double[] allocateBlock(int size) {
        return new double[size];
    }

    @Override
    void resetBlocks(double[][] newBlocks, double[][] newPrev) {
        blocks = newBlocks;
        prevBlocks = newPrev;
    }

    @Override
    double[][] getPrevBlocks() {
        return prevBlocks;
    }

    @Override
    SoftRecycler<double[]> getRecycler() {
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
        final double [] backingArray = blocks[blockNo];
        chunk.asResettableWritableDoubleChunk().resetFromTypedArray(backingArray, 0, BLOCK_SIZE);
        return ((long)blockNo) << LOG_BLOCK_SIZE;
    }

    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        Assert.eqNull(prevInUse, "prevInUse");
        final int blockNo = getBlockNo(position);
        final double [] backingArray = blocks[blockNo];
        final long firstPosition = ((long) blockNo) << LOG_BLOCK_SIZE;
        final int offset = (int)(position - firstPosition);
        final int capacity = BLOCK_SIZE - offset;
        chunk.asResettableWritableDoubleChunk().resetFromTypedArray(backingArray, offset, capacity);
        return capacity;
    }

    @Override
    protected void fillSparseChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {
        if (indices.size() == 0) {
            destGeneric.setSize(0);
            return;
        }
        final WritableDoubleChunk<? super Values> dest = destGeneric.asWritableDoubleChunk();
        final FillSparseChunkContext<double[]> ctx = new FillSparseChunkContext<>();
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

        final WritableDoubleChunk<? super Values> dest = destGeneric.asWritableDoubleChunk();
        final FillSparseChunkContext<double[]> ctx = new FillSparseChunkContext<>();
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
            dest.set(ctx.offset++, usePrev ? ctx.currentPrevBlock[indexWithinBlock] : ctx.currentBlock[indexWithinBlock]);
        });
        dest.setSize(ctx.offset);
    }

    @Override
    protected void fillSparseChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
        final WritableDoubleChunk<? super Values> dest = destGeneric.asWritableDoubleChunk();
        final int sz = indices.size();
        for (int ii = 0; ii < sz; ++ii) {
            final long fromIndex = indices.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                dest.set(ii, NULL_DOUBLE);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo >= blocks.length) {
                dest.set(ii, NULL_DOUBLE);
            } else {
                final double[] currentBlock = blocks[blockNo];
                dest.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
            }
        }
        dest.setSize(sz);
    }

    @Override
    protected void fillSparsePrevChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
        final WritableDoubleChunk<? super Values> dest = destGeneric.asWritableDoubleChunk();
        final int sz = indices.size();
        for (int ii = 0; ii < sz; ++ii) {
            final long fromIndex = indices.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                dest.set(ii, NULL_DOUBLE);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo >= blocks.length) {
                dest.set(ii, NULL_DOUBLE);
                continue;
            }
            final double[] currentBlock = shouldUsePrevious(fromIndex) ? prevBlocks[blockNo] : blocks[blockNo];
            dest.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
        }
        dest.setSize(sz);
    }

    @Override
    void fillFromChunkByRanges(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        if (rowSequence.size() == 0) {
            return;
        }
        final DoubleChunk<? extends Values> chunk = src.asDoubleChunk();
        final LongChunk<OrderedRowKeyRanges> ranges = rowSequence.asRowKeyRangesChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        int offset = 0;
        // This helps us reduce the number of calls to Chunk.isAlias
        double[] knownUnaliasedBlock = null;
        for (int ii = 0; ii < ranges.size(); ii += 2) {
            long firstKey = ranges.get(ii);
            final long lastKey = ranges.get(ii + 1);

            while (firstKey <= lastKey) {
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
                final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
                final int length = (int) (lastKeyToUse - firstKey + 1);

                final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
                final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
                final double[] inner = blocks[block];

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
        final DoubleChunk<? extends Values> chunk = src.asDoubleChunk();

        final long lastKey = firstKey + totalLength - 1;

        while (firstKey <= lastKey) {
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
            final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
            final int length = (int) (lastKeyToUse - firstKey + 1);

            final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
            final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
            final double[] inner = blocks[block];

            chunk.copyToTypedArray(offset, inner, sIndexWithinBlock, length);
            firstKey += length;
            offset += length;
        }
    }

    @Override
    void fillFromChunkByKeys(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        if (rowSequence.size() == 0) {
            return;
        }
        final DoubleChunk<? extends Values> chunk = src.asDoubleChunk();
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
            final double[] inner = blocks[block];

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
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        if (keys.size() == 0) {
            return;
        }
        final DoubleChunk<? extends Values> chunk = src.asDoubleChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long minKeyInCurrentBlock = firstKey & ~INDEX_MASK;
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

            final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
            final double[] inner = blocks[block];

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
