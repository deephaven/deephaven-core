/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import gnu.trove.list.array.TIntArrayList;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.vector.Vector;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.SoftRecycler;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class ObjectArraySource<T> extends ArraySourceHelper<T, T[]> implements MutableColumnSourceGetDefaults.ForObject<T> {
    @SuppressWarnings("rawtypes")
    private static final SoftRecycler recycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new Object[BLOCK_SIZE], (item) -> Arrays.fill(item, null));
    transient private T[][] prevBlocks;
    private T[][] blocks;
    private final boolean isArrayType;

    public ObjectArraySource(Class<T> type) {
        super(type);
        isArrayType = Vector.class.isAssignableFrom(type);
        init();
    }

    public ObjectArraySource(Class<T> type, Class<?> componentType) {
        super(type, componentType);
        isArrayType = Vector.class.isAssignableFrom(type);
        init();
    }

    @Override
    public void startTrackingPrevValues() {
        super.startTrackingPrev(blocks.length);
        //noinspection unchecked
        prevBlocks = (T[][]) new Object[blocks.length][];
    }

    private void init() {
        //noinspection unchecked
        blocks = (T[][]) new Object[INITIAL_NUMBER_OF_BLOCKS][];
        maxIndex = INITIAL_MAX_INDEX;
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        ensureCapacity(capacity, blocks, prevBlocks, nullFill);
    }

    /**
     * This version of `prepareForParallelPopulation` will internally call {@link #ensureCapacity(long, boolean)} to
     * make sure there is room for the incoming values.
     *
     * @param changedIndices indices in the dense table
     */
    @Override
    public void prepareForParallelPopulation(RowSequence changedIndices) {
        final long currentStep = LogicalClock.DEFAULT.currentStep();
        if (ensurePreviousClockCycle == currentStep) {
            throw new IllegalStateException("May not call ensurePrevious twice on one clock cycle!");
        }
        ensurePreviousClockCycle = currentStep;

        if (changedIndices.isEmpty()) {
            return;
        }

        // ensure that this source will have sufficient capacity to store these indices, does not need to be
        // null-filled as the values will be immediately written
        ensureCapacity(changedIndices.lastRowKey() + 1, false);

        if (prevFlusher != null) {
            prevFlusher.maybeActivate();
        } else {
            // we are not tracking this source yet so we have nothing to do for the previous values
            return;
        }

        try (final RowSequence.Iterator it = changedIndices.getRowSequenceIterator()) {
            do {
                final long firstKey = it.peekNextKey();

                final int block = (int) (firstKey >> LOG_BLOCK_SIZE);

                final long[] inUse;
                if (prevBlocks[block] == null) {
                    prevBlocks[block] = (T[]) recycler.borrowItem();
                    prevInUse[block] = inUse = inUseRecycler.borrowItem();
                    if (prevAllocated == null) {
                        prevAllocated = new TIntArrayList();
                    }
                    prevAllocated.add(block);
                } else {
                    inUse = prevInUse[block];
                }

                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

                it.getNextRowSequenceThrough(maxKeyInCurrentBlock).forAllRowKeys(key -> {
                    final int nextIndexWithinBlock = (int) (key & INDEX_MASK);
                    final int nextIndexWithinInUse = nextIndexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                    final long nextMaskWithinInUse = 1L << (nextIndexWithinBlock & IN_USE_MASK);
                    prevBlocks[block][nextIndexWithinBlock] = blocks[block][nextIndexWithinBlock];
                    inUse[nextIndexWithinInUse] |= nextMaskWithinInUse;
                });
            } while (it.hasMore());
        }
    }

    @Override
    public void setNull(long key) {
        set(key, null);
    }

    @Override
    public void set(long key, T value) {
        final int block = (int) (key >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (key & INDEX_MASK);
        if (shouldRecordPrevious(key, prevBlocks, recycler)) {
            prevBlocks[block][indexWithinBlock] = blocks[block][indexWithinBlock];
        }
        blocks[block][indexWithinBlock] = value;
    }

    @Override
    final public T get(long rowKey) {
        if (rowKey < 0 || rowKey > maxIndex) {
            return null;
        }
        return getUnsafe(rowKey);
    }

    final public T getUnsafe(long index) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        return blocks[blockIndex][indexWithinBlock];
    }

    public final T getAndSetUnsafe(long index, T newValue) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        final T oldValue = blocks[blockIndex][indexWithinBlock];
        // not a perfect comparison, but very cheap
        if (oldValue == newValue) {
            return oldValue;
        }
        //noinspection unchecked
        if (shouldRecordPrevious(index, prevBlocks, recycler)) {
            prevBlocks[blockIndex][indexWithinBlock] = oldValue;
        }
        blocks[blockIndex][indexWithinBlock] = newValue;
        return oldValue;
    }

    @Override
    public T getPrev(long rowKey) {
        if (rowKey < 0 || rowKey > maxIndex) {
            return null;
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
    final T[] allocateNullFilledBlock(int size) {
        //noinspection unchecked
        return (T[])new Object[size];
    }

    @Override
    final T[] allocateBlock(int size) {
        //noinspection unchecked
        return (T[])new Object[size];
    }

    @Override
    void resetBlocks(T[][] newBlocks, T[][] newPrev) {
        this.blocks = newBlocks;
        this.prevBlocks = newPrev;
    }

    @Override
    T[][] getPrevBlocks() {
        return prevBlocks;
    }

    @Override
    SoftRecycler<T[]> getRecycler() {
        //noinspection unchecked
        return (SoftRecycler<T[]>) recycler;
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
        final T [] backingArray = blocks[blockNo];
        chunk.asResettableWritableObjectChunk().resetFromTypedArray(backingArray, 0, BLOCK_SIZE);
        return (long)blockNo << LOG_BLOCK_SIZE;
    }

    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        Assert.eqNull(prevInUse, "prevInUse");
        final int blockNo = getBlockNo(position);
        final T [] backingArray = blocks[blockNo];
        final long firstPosition = ((long) blockNo) << LOG_BLOCK_SIZE;
        final int offset = (int)(position - firstPosition);
        final int capacity = BLOCK_SIZE - offset;
        chunk.asResettableWritableObjectChunk().resetFromTypedArray(backingArray, offset, capacity);
        return capacity;
    }


    @Override
    protected void fillSparseChunk(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final RowSequence indices) {
        final long sz = indices.size();
        if (sz == 0) {
            destGeneric.setSize(0);
            return;
        }
        final WritableObjectChunk<T, ? super Values> dest = destGeneric.asWritableObjectChunk();
        final FillSparseChunkContext<T[]> ctx = new FillSparseChunkContext<>();
        indices.forEachRowKey((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1L) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
            }
            dest.set(ctx.offset++, ctx.currentBlock[(int) (v & INDEX_MASK)]);
            return true;
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

        final WritableObjectChunk<T, ? super Values> dest = destGeneric.asWritableObjectChunk();
        final FillSparseChunkContext<T[]> ctx = new FillSparseChunkContext<>();
        indices.forEachRowKey((final long v) -> {
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
            return true;
        });
        dest.setSize(ctx.offset);
    }

    @Override
    protected void fillSparseChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
        final WritableObjectChunk<T, ? super Values> dest = destGeneric.asWritableObjectChunk();
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
                final T[] currentBlock = blocks[blockNo];
                dest.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
            }
        }
        dest.setSize(sz);
    }

    @Override
    protected void fillSparsePrevChunkUnordered(@NotNull final WritableChunk<? super Values> destGeneric, @NotNull final LongChunk<? extends RowKeys> indices) {
        final WritableObjectChunk<T, ? super Values> dest = destGeneric.asWritableObjectChunk();
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

            final T[] currentBlock = shouldUsePrevious(fromIndex) ? prevBlocks[blockNo] : blocks[blockNo];
            dest.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
        }
        dest.setSize(sz);
    }

    @Override
    void fillFromChunkByRanges(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        final ObjectChunk<T, ? extends Values> chunk = src.asObjectChunk();
        final LongChunk<OrderedRowKeyRanges> ranges = rowSequence.asRowKeyRangesChunk();

        final boolean trackPrevious = prevFlusher != null && ensurePreviousClockCycle != LogicalClock.DEFAULT.currentStep();

        if (trackPrevious) {
            prevFlusher.maybeActivate();
        }

        int offset = 0;
        // This helps us reduce the number of calls to Chunk.isAlias
        T[] knownUnaliasedBlock = null;
        for (int ii = 0; ii < ranges.size(); ii += 2) {
            long firstKey = ranges.get(ii);
            final long lastKey = ranges.get(ii + 1);

            while (firstKey <= lastKey) {
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
                final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
                final int length = (int) (lastKeyToUse - firstKey + 1);

                final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
                final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
                final T[] inner = blocks[block];

                if (inner != knownUnaliasedBlock && chunk.isAlias(inner)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
                knownUnaliasedBlock = inner;

                // This 'if' with its constant condition should be very friendly to the branch predictor.
                if (trackPrevious) {
                    // this should be vectorized
                    for (int jj = 0; jj < length; ++jj) {
                        if (shouldRecordPrevious(firstKey + jj, prevBlocks, recycler)) {
                            prevBlocks[block][sIndexWithinBlock + jj] = inner[sIndexWithinBlock + jj];
                        }
                    }
                }

                // region copyToTypedArray
                chunk.copyToTypedArray(offset, inner, sIndexWithinBlock, length);
                // endregion copyToTypedArray
                firstKey += length;
                offset += length;
            }
        }
    }


    public void copyFromChunk(long firstKey, long totalLength, Chunk<? extends Values> src, int offset) {
        if (totalLength == 0) {
            return;
        }
        final ObjectChunk<T, ? extends Values> chunk = src.asObjectChunk();

        final long lastKey = firstKey + totalLength - 1;

        while (firstKey <= lastKey) {
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
            final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
            final int length = (int) (lastKeyToUse - firstKey + 1);

            final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
            final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
            final T [] inner = blocks[block];

            chunk.copyToTypedArray(offset, inner, sIndexWithinBlock, length);
            firstKey += length;
            offset += length;
        }
    }

    @Override
    void fillFromChunkByKeys(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        final ObjectChunk<T, ? extends Values> chunk = src.asObjectChunk();
        final LongChunk<OrderedRowKeys> keys = rowSequence.asRowKeyChunk();

        final boolean trackPrevious = prevFlusher != null && ensurePreviousClockCycle != LogicalClock.DEFAULT.currentStep();

        if (trackPrevious) {
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
            final T[] inner = blocks[block];

            if (chunk.isAlias(inner)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            while (ii <= lastII) {
                final long key = keys.get(ii);
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (trackPrevious) {
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
        final ObjectChunk<T, ? extends Values> chunk = src.asObjectChunk();

        final boolean trackPrevious = prevFlusher != null && ensurePreviousClockCycle != LogicalClock.DEFAULT.currentStep();

        if (trackPrevious) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long minKeyInCurrentBlock = firstKey & ~INDEX_MASK;
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

            final int block = (int) (firstKey >> LOG_BLOCK_SIZE);
            final T [] inner = blocks[block];

            if (chunk.isAlias(inner)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            long key = keys.get(ii);
            do {
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (trackPrevious) {
                    if (shouldRecordPrevious(key, prevBlocks, recycler)) {
                        prevBlocks[block][indexWithinBlock] = inner[indexWithinBlock];
                    }
                }
                inner[indexWithinBlock] = chunk.get(ii);
                ++ii;
            } while (ii < keys.size() && (key = keys.get(ii)) >= minKeyInCurrentBlock && key <= maxKeyInCurrentBlock);
        }
    }

    public void move(long source, long dest, long length) {
        if (prevBlocks != null) {
            throw new UnsupportedOperationException();
        }
        if (source == dest) {
            return;
        }
        if ((source - dest) % BLOCK_SIZE == 0) {
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

                blocks[destBlock][destIndexWithinBlock] = blocks[sourceBlock][sourceIndexWithinBlock];

                // TODO: figure out the first key in both blocks, and do an array copy

                ii -= 1;
            }
        } else {
            for (long ii = 0; ii < length;) {
                final long sourceKey = source + ii;
                final long destKey = dest + ii;
                final int sourceBlock = (int) (sourceKey >> LOG_BLOCK_SIZE);
                final int sourceIndexWithinBlock = (int) (sourceKey & INDEX_MASK);

                final int destBlock = (int) (destKey >> LOG_BLOCK_SIZE);
                final int destIndexWithinBlock = (int) (destKey & INDEX_MASK);

                blocks[destBlock][destIndexWithinBlock] = blocks[sourceBlock][sourceIndexWithinBlock];

                // TODO: figure out the last key in both blocks, and do an array copy

                ii += 1;
            }
        }
    }
}
