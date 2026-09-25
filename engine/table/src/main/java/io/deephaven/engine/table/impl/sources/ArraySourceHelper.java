//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.util.copy.CopyKernel;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.util.SoftRecycler;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

abstract class ArraySourceHelper<T, UArray> extends ArrayBackedColumnSource<T>
        implements WritableSourceWithPrepareForParallelPopulation {
    /**
     * The presence of a prevFlusher means that this ArraySource wants to track previous values. If prevFlusher is null,
     * the ArraySource does not want (or does not yet want) to track previous values. Deserialized ArraySources never
     * track previous values.
     */
    protected transient UpdateCommitter<ArraySourceHelper<T, UArray>> prevFlusher = null;
    protected transient IntArrayList prevAllocated = null;

    /**
     * If ensure previous has been called, we need not check previous values when filling.
     */
    protected transient long ensurePreviousClockCycle = -1;

    /**
     * Whether {@link #ensureCapacity} last allocated null-filled blocks, rather than blocks of the element type's
     * default. The few blocks {@link #moveWholeBlocks} must allocate are allocated the same way, so their positions
     * start from the value their owner expects of newly allocated ones.
     */
    private transient boolean freshBlocksNullFilled = true;

    ArraySourceHelper(Class<T> type) {
        super(type);
    }

    ArraySourceHelper(Class<T> type, Class<?> componentType) {
        super(type, componentType);
    }

    static class FillContext implements ColumnSource.FillContext {
        final CopyKernel copyKernel;

        FillContext(ChunkType chunkType) {
            this.copyKernel = CopyKernel.makeCopyKernel(chunkType);
        }

        @Override
        public boolean supportsUnboundedFill() {
            return true;
        }
    }

    @Override
    public ChunkSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return makeFillContext(getChunkType());
    }

    @NotNull
    FillContext makeFillContext(ChunkType chunkType) {
        return new FillContext(chunkType);
    }

    /**
     * Get the capacity of this column source. This number is one higher than the highest key that may be accessed (read
     * or written).
     *
     * @return The capacity of this column source
     */
    public final long getCapacity() {
        return maxIndex + 1;
    }

    final void ensureCapacity(final long capacity, UArray[] blocks, UArray[] prevBlocks) {
        ensureCapacity(capacity, blocks, prevBlocks);
    }

    /**
     * This method supports the 'ensureCapacity' method for all of this class' inheritors.
     */
    final void ensureCapacity(final long capacity, UArray[] blocks, UArray[] prevBlocks, boolean nullFilled) {
        freshBlocksNullFilled = nullFilled;
        // Convert requested capacity to requestedMaxIndex and requestedNumBlocks, but leave early if the requested
        // maxIndex is <= the current maxIndex.
        //
        // Rationale for this formula:
        // capacity, rounded up to the next blockSize, then -1 to form a max
        final long requestedMaxIndex = ((capacity + BLOCK_SIZE - 1) & ~INDEX_MASK) - 1;
        if (requestedMaxIndex <= maxIndex) {
            return;
        }
        final long requestedNumBlocksLong = (requestedMaxIndex + 1) >> LOG_BLOCK_SIZE;
        final int requestedNumBlocks =
                LongSizedDataStructure.intSize("ArrayBackedColumnSource block allocation", requestedNumBlocksLong);

        // If we don't have enough blocks, reallocate the array
        if (blocks.length < requestedNumBlocks) {
            int roundedNumBlocks = Math.max(blocks.length, 1);
            do {
                roundedNumBlocks *= 2;
            } while (roundedNumBlocks < requestedNumBlocks);
            blocks = Arrays.copyOf(blocks, roundedNumBlocks);
            if (prevFlusher != null) {
                prevBlocks = Arrays.copyOf(prevBlocks, roundedNumBlocks);
                prevInUse = Arrays.copyOf(prevInUse, roundedNumBlocks);
            }
            resetBlocks(blocks, prevBlocks);
        }

        // We know how many blocks we have allocated by looking at maxIndex. This may well be less than the size of the
        // 'blocks' array because we only allocate blocks as needed.
        final int allocatedNumBlocks = (int) ((maxIndex + 1) >> LOG_BLOCK_SIZE);

        // Allocate storage up to 'requestedNumBlocks' (not roundedNumBlocks). The difference is that the array size may
        // double, but we only allocate the minimum number of blocks needed. Put another way, we only allocate blocks up
        // to the requested capacity, not all the way up to (the capacity rounded to the next power of two).
        for (int ii = allocatedNumBlocks; ii < requestedNumBlocks; ++ii) {
            if (nullFilled) {
                blocks[ii] = allocateNullFilledBlock(BLOCK_SIZE);
            } else {
                blocks[ii] = allocateBlock(BLOCK_SIZE);
            }
        }
        // Note: if we get this far, requestedMaxIndex > maxIndex, so this will always increase maxIndex.
        maxIndex = requestedMaxIndex;
    }

    /**
     * This method supports the 'set' method for its inheritors, doing some of the 'inUse' housekeeping that is common
     * to all inheritors.
     *
     * @return true if the inheritor should copy a value from current to prev before setting current; false if it should
     *         just set a current value without touching prev.
     */
    final boolean shouldRecordPrevious(final long key, final UArray[] prevBlocks,
            final SoftRecycler<UArray> recycler) {
        if (prevFlusher == null) {
            return false;
        }
        // If we want to track previous values, we make sure we are registered with the PeriodicUpdateGraph.
        prevFlusher.maybeActivate();

        final int block = (int) (key >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (key & INDEX_MASK);
        final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
        final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

        boolean shouldRecordPrev = false;

        // prevFlusher != null means we are tracking previous values.
        final long[] inUse;
        if (prevBlocks[block] == null) {
            prevBlocks[block] = recycler.borrowItem();
            prevInUse[block] = inUse = inUseRecycler.borrowItem();
            if (prevAllocated == null) {
                prevAllocated = new IntArrayList();
            }
            prevAllocated.add(block);
        } else {
            inUse = prevInUse[block];
        }
        // Set value only if not already in use
        if ((inUse[indexWithinInUse] & maskWithinInUse) == 0) {
            shouldRecordPrev = true;
            inUse[indexWithinInUse] |= maskWithinInUse;
        }

        return shouldRecordPrev;
    }

    /**
     * Force my inheritors to implement this method, rather than taking the interface default.
     */
    @Override
    public abstract void startTrackingPrevValues();

    final void startTrackingPrev(int numBlocks) {
        if (prevFlusher != null) {
            throw new IllegalStateException("Can't call startTrackingPrevValues() twice: " +
                    this.getClass().getCanonicalName());
        }
        prevFlusher = new UpdateCommitter<>(this, updateGraph,
                ArraySourceHelper::commitBlocks);
        prevInUse = new long[numBlocks][];
    }

    /**
     * This method supports the 'getPrev' method for its inheritors, doing some of the 'inUse' housekeeping that is
     * common to all inheritors.
     *
     * @return true if the inheritor should return a value from its "prev" data structure; false if it should return a
     *         value from its "current" data structure.
     */
    final boolean shouldUsePrevious(final long rowKey) {
        if (prevFlusher == null) {
            return false;
        }
        final int blockIndex = (int) (rowKey >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (rowKey & INDEX_MASK);
        final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
        final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);
        final long[] inUse = prevInUse[blockIndex];
        return inUse != null && (inUse[indexWithinInUse] & maskWithinInUse) != 0;
    }

    private void commitBlocks() {
        if (prevAllocated == null) {
            return;
        }

        final UArray[] prevBlocks = getPrevBlocks();
        final SoftRecycler<UArray> recycler = getRecycler();
        Assert.eq(prevBlocks.length, "prevBlocks.length", prevInUse.length, "prevInUse.length");

        prevAllocated.forEach(block -> {
            final UArray pb = prevBlocks[block];
            assert pb != null;
            recycler.returnItem(pb);
            prevBlocks[block] = null;

            final long[] pu = prevInUse[block];
            assert pu != null;
            inUseRecycler.returnItem(pu);
            prevInUse[block] = null;
        });
        prevAllocated.clear();
    }

    @Override
    public FillFromContext makeFillFromContext(int chunkCapacity) {
        return DEFAULT_FILL_FROM_INSTANCE;
    }

    @Override
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillFromChunkByKeys(rowSequence, src);
        } else {
            fillFromChunkByRanges(rowSequence, src);
        }
    }

    abstract void fillFromChunkByRanges(@NotNull RowSequence rowSequence, Chunk<? extends Values> src);

    abstract void fillFromChunkByKeys(@NotNull RowSequence rowSequence, Chunk<? extends Values> src);

    abstract UArray allocateNullFilledBlock(int size);

    abstract UArray allocateBlock(int size);

    abstract void resetBlocks(UArray[] newBlocks, UArray[] newPrev);

    abstract UArray[] getPrevBlocks();

    /**
     * @return the array of current-value blocks, whose elements may be replaced
     */
    abstract UArray[] getBlocks();

    /**
     * Move whole blocks of values, by moving the blocks themselves rather than their values where possible. Both
     * positions must be at the start of a block; any partial block at the end of the range is left for the caller.
     * Source blocks that are not also destinations are left unallocated: the owner of the addresses assigns no values
     * there without first releasing storage through the end of the capacity, so that {@link #ensureCapacity} allocates
     * it again. A destination within the capacity whose source never was allocated receives a newly allocated block,
     * since positions there may be assigned without the capacity changing.
     *
     * <p>
     * When previous values are tracked, each affected block's previous values are first made complete for this cycle: a
     * block with none recorded yet gives its current array to its previous values, and a block with some recorded has
     * the rest copied. A destination then receives its source block's array, or a copy of it where that array now holds
     * previous values, so no array is both a current and a previous block.
     * </p>
     *
     * @param source the first source position, at the start of a block
     * @param dest the first destination position, at the start of a block
     * @param length the number of positions to move
     * @return the number of positions moved, a multiple of the block size
     */
    final long moveWholeBlocks(final long source, final long dest, final long length) {
        final int blockCount = (int) (length >> LOG_BLOCK_SIZE);
        if (blockCount == 0 || source == dest) {
            return 0;
        }
        final int sourceBlock = (int) (source >> LOG_BLOCK_SIZE);
        final int destBlock = (int) (dest >> LOG_BLOCK_SIZE);
        // blocks at or past this one have never been allocated; their positions hold no values
        final int allocatedBlocks = (int) ((maxIndex + 1) >> LOG_BLOCK_SIZE);
        final UArray[] blocks = getBlocks();
        final boolean down = destBlock < sourceBlock;
        if (prevFlusher == null) {
            // move blocks in the order that never overwrites a block still to be moved
            for (int step = 0; step < blockCount; ++step) {
                final int offset = down ? step : blockCount - 1 - step;
                final int from = sourceBlock + offset;
                final int to = destBlock + offset;
                if (to < allocatedBlocks) {
                    blocks[to] = from < allocatedBlocks ? blocks[from] : allocateFreshBlock();
                }
            }
            for (int bi = sourceBlock; bi < Math.min(sourceBlock + blockCount, allocatedBlocks); ++bi) {
                final boolean isDest = bi >= destBlock && bi < destBlock + blockCount;
                if (!isDest) {
                    blocks[bi] = null;
                }
            }
            return (long) blockCount << LOG_BLOCK_SIZE;
        }

        prevFlusher.maybeActivate();
        final int firstBlock = Math.min(sourceBlock, destBlock);
        final int lastBlock = Math.min(Math.max(sourceBlock, destBlock) + blockCount, allocatedBlocks) - 1;
        final UArray[] prevBlocks = getPrevBlocks();
        final SoftRecycler<UArray> recycler = getRecycler();
        // the current values before the move, for each affected block, and whether that array now holds previous values
        final Object[] oldCurrent = new Object[Math.max(0, lastBlock - firstBlock + 1)];
        final boolean[] givenToPrevious = new boolean[oldCurrent.length];
        for (int bi = firstBlock; bi <= lastBlock; ++bi) {
            final UArray current = blocks[bi];
            if (current == null) {
                // a released block holds no live values, so it has no previous values to keep
                continue;
            }
            if (prevBlocks[bi] == null) {
                prevBlocks[bi] = current;
                final long[] inUse = inUseRecycler.borrowItem();
                Arrays.fill(inUse, -1L);
                prevInUse[bi] = inUse;
                if (prevAllocated == null) {
                    prevAllocated = new IntArrayList();
                }
                prevAllocated.add(bi);
                givenToPrevious[bi - firstBlock] = true;
            } else {
                completePrevious(current, prevBlocks[bi], prevInUse[bi]);
            }
            oldCurrent[bi - firstBlock] = current;
            blocks[bi] = null;
        }
        for (int offset = 0; offset < blockCount; ++offset) {
            final int from = sourceBlock + offset;
            final int to = destBlock + offset;
            if (to >= allocatedBlocks) {
                continue;
            }
            if (from >= allocatedBlocks) {
                blocks[to] = allocateFreshBlock();
                continue;
            }
            // noinspection unchecked
            final UArray moved = (UArray) oldCurrent[from - firstBlock];
            if (moved == null || !givenToPrevious[from - firstBlock]) {
                // released, so the destination is too; or the previous values have an array of their own
                blocks[to] = moved;
                continue;
            }
            final UArray copy = recycler.borrowItem();
            // noinspection SuspiciousSystemArraycopy
            System.arraycopy(moved, 0, copy, 0, BLOCK_SIZE);
            blocks[to] = copy;
        }
        return (long) blockCount << LOG_BLOCK_SIZE;
    }

    /**
     * Allocate the block at {@code blockIndex} if a move left it unallocated, before values are copied into part of it.
     */
    final void allocateIfMissing(final int blockIndex) {
        final UArray[] blocks = getBlocks();
        if (blockIndex < blocks.length && blocks[blockIndex] == null) {
            blocks[blockIndex] = allocateFreshBlock();
        }
    }

    private UArray allocateFreshBlock() {
        return freshBlocksNullFilled ? allocateNullFilledBlock(BLOCK_SIZE) : allocateBlock(BLOCK_SIZE);
    }

    /**
     * Copy into {@code prev} every value of {@code current} whose previous value has not been recorded this cycle, and
     * mark the whole block recorded.
     */
    private static void completePrevious(final Object current, final Object prev, final long[] inUse) {
        for (int word = 0; word < inUse.length; ++word) {
            long missing = ~inUse[word];
            while (missing != 0) {
                // copy each run of unrecorded values
                final int start = Long.numberOfTrailingZeros(missing);
                final int end = Math.min(Long.SIZE, start + Long.numberOfTrailingZeros(~(missing >>> start)));
                final int first = (word << LOG_INUSE_BITSET_SIZE) + start;
                // noinspection SuspiciousSystemArraycopy
                System.arraycopy(current, first, prev, first, end - start);
                missing = end >= Long.SIZE ? 0 : missing & (-1L << end);
            }
            inUse[word] = -1L;
        }
    }

    /**
     * Drop the current-value storage for a block. Previous-value storage is left for {@link #commitBlocks()}.
     *
     * @param blockIndex the block to release
     */
    abstract void releaseBlock(int blockIndex);

    @Override
    public void releaseBlocks(final long firstKey, final long lastKey) {
        final long firstBlock = (firstKey + BLOCK_SIZE - 1) >> LOG_BLOCK_SIZE;
        final long allocatedBlocks = (maxIndex + 1) >> LOG_BLOCK_SIZE;
        final long endBlock = Math.min((lastKey + 1) >> LOG_BLOCK_SIZE, allocatedBlocks);
        for (long bi = firstBlock; bi < endBlock; ++bi) {
            releaseBlock((int) bi);
        }
        if (lastKey >= maxIndex && firstBlock < allocatedBlocks) {
            // released through the end, so the capacity shrinks and ensureCapacity allocates these blocks again
            maxIndex = (firstBlock << LOG_BLOCK_SIZE) - 1;
        }
    }

    abstract SoftRecycler<UArray> getRecycler();

    protected static class FillSparseChunkContext<UArray> {
        int offset;
        int currentBlockNo;
        long capForCurrentBlock = -1;
        UArray currentBlock;
        UArray currentPrevBlock;
        long[] prevInUseBlock;
    }
}
