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
import io.deephaven.engine.updategraph.LogicalClock;
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
     * The in-use bitset of every block allocated during the current update cycle, with every bit set, so that writes to
     * the block record no previous values and reads of its previous values see the {@link #freshPrevBlock read-only
     * block} of the values it was allocated with. It is never written.
     */
    static final long[] FRESH_IN_USE = makeFreshInUse();
    /** The value of {@link #firstFreshBlock} when no block has been allocated during the current update cycle. */
    private static final int NO_FIRST_FRESH_BLOCK = Integer.MAX_VALUE;
    /**
     * The first block allocated during the current update cycle, or {@link #NO_FIRST_FRESH_BLOCK} if none was. Blocks
     * are only ever allocated after every other, so every block allocated during the cycle is at or after it, and
     * commit scans upward from it to clear their previous-value entries.
     */
    private transient int firstFreshBlock = NO_FIRST_FRESH_BLOCK;

    private static long[] makeFreshInUse() {
        final long[] inUse = new long[IN_USE_BLOCK_SIZE];
        Arrays.fill(inUse, -1L);
        return inUse;
    }

    /**
     * If ensure previous has been called, we need not check previous values when filling.
     */
    protected transient long ensurePreviousClockCycle = -1;

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
        // No row key in a block allocated during an update cycle existed before it, so its previous values are the
        // values it is allocated with, which a shared read-only block holds; between cycles they are its values.
        final boolean markFresh = prevFlusher != null && allocatedNumBlocks < requestedNumBlocks
                && updateGraph.clock().currentState() == LogicalClock.State.Updating;
        for (int ii = allocatedNumBlocks; ii < requestedNumBlocks; ++ii) {
            if (nullFilled) {
                blocks[ii] = allocateNullFilledBlock(BLOCK_SIZE);
            } else {
                blocks[ii] = allocateBlock(BLOCK_SIZE);
            }
            if (markFresh) {
                prevBlocks[ii] = freshPrevBlock(nullFilled);
                prevInUse[ii] = FRESH_IN_USE;
            }
        }
        if (markFresh) {
            // This correctly updates the minimum both in the initial case (where firstFreshBlock ==
            // NO_FIRST_FRESH_BLOCK) and subsequent cases.
            firstFreshBlock = Math.min(firstFreshBlock, allocatedNumBlocks);
            prevFlusher.maybeActivate();
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
        if (firstFreshBlock != NO_FIRST_FRESH_BLOCK) {
            final UArray[] prevBlocks = getPrevBlocks();
            final int allocatedBlocks = (int) Math.min(prevInUse.length, (maxIndex + 1) >> LOG_BLOCK_SIZE);
            // a block the range skipped, as ensureCapacityLike does, was never pointed at the shared entries
            for (int block = firstFreshBlock; block < allocatedBlocks; ++block) {
                if (prevInUse[block] == FRESH_IN_USE) {
                    prevBlocks[block] = null;
                    prevInUse[block] = null;
                }
            }
            firstFreshBlock = NO_FIRST_FRESH_BLOCK;
        }
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

    /**
     * @param nullFilled whether the block's values were allocated null-filled, rather than the element type's default
     * @return a block, shared and never written, of the values a block is allocated with, as the previous values of a
     *         block allocated during the current update cycle
     */
    abstract UArray freshPrevBlock(boolean nullFilled);

    /**
     * @return whether the block was allocated during the current update cycle, so that its previous values are the ones
     *         it was allocated with and must not be written
     */
    final boolean isFreshBlock(final int block) {
        return prevInUse[block] == FRESH_IN_USE;
    }

    abstract UArray allocateBlock(int size);

    abstract void resetBlocks(UArray[] newBlocks, UArray[] newPrev);

    abstract UArray[] getPrevBlocks();

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
