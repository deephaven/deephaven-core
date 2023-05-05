/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterSparseArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.engine.table.impl.sources.sparse.ShortOneOrN;
import io.deephaven.engine.table.impl.sources.sparse.LongOneOrN;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.SoftRecycler;
import gnu.trove.list.array.TLongArrayList;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

// region boxing imports
import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static io.deephaven.util.type.TypeUtils.box;
import static io.deephaven.util.type.TypeUtils.unbox;
// endregion boxing imports

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.*;

/**
 * Sparse array source for Short.
 * <p>
 * The C-haracterSparseArraySource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ShortSparseArraySource extends SparseArrayColumnSource<Short>
        implements MutableColumnSourceGetDefaults.ForShort /* MIXIN_IMPLS */ {
    // region recyclers
    private static final SoftRecycler<short[]> recycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new short[BLOCK_SIZE], null);
    private static final SoftRecycler<short[][]> recycler2 = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new short[BLOCK2_SIZE][], null);
    private static final SoftRecycler<ShortOneOrN.Block2[]> recycler1 = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new ShortOneOrN.Block2[BLOCK1_SIZE], null);
    private static final SoftRecycler<ShortOneOrN.Block1[]> recycler0 = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new ShortOneOrN.Block1[BLOCK0_SIZE], null);
    // endregion recyclers

    /**
     * The presence of a prevFlusher means that this ArraySource wants to track previous values. If prevFlusher is null,
     * the ArraySource does not want (or does not yet want) to track previous values. Deserialized ArraySources never
     * track previous values.
     */
    protected transient UpdateCommitter<ShortSparseArraySource> prevFlusher = null;

    /**
     * If prepareForParallelPopulation has been called, we need not check previous values when filling.
     */
    private transient long prepareForParallelPopulationClockCycle = -1;

    /**
     * Our previous page table could be very sparse, and we do not want to read through millions of nulls to find out
     * what blocks to recycle.  Instead we maintain a list of blocks that we have allocated (as the key shifted by
     * BLOCK0_SHIFT).  We recycle those blocks in the PrevFlusher; and accumulate the set of blocks that must be
     * recycled from the next level array, and so on until we recycle the top-level prevBlocks and prevInUse arrays.
     */
    private transient final TLongArrayList blocksToFlush = new TLongArrayList();

    protected ShortOneOrN.Block0 blocks;
    protected transient ShortOneOrN.Block0 prevBlocks;

    // region constructor
    public ShortSparseArraySource() {
        super(short.class);
        blocks = new ShortOneOrN.Block0();
    }
    // endregion constructor

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        // Nothing to do here. Sparse array sources allocate on-demand and always null-fill.
    }

    // region setNull
    @Override
    public void setNull(long key) {
        final short [] blocks2 = blocks.getInnermostBlockByKeyOrNull(key);
        if (blocks2 == null) {
            return;
        }
        final int indexWithinBlock = (int) (key & INDEX_MASK);
        if (blocks2[indexWithinBlock] == NULL_SHORT) {
            return;
        }

        final short [] prevBlocksInner = shouldRecordPrevious(key);
        if (prevBlocksInner != null) {
            prevBlocksInner[indexWithinBlock] = blocks2[indexWithinBlock];
        }
        blocks2[indexWithinBlock] = NULL_SHORT;
    }
    // endregion setNull

    @Override
    public final void set(long key, short value) {
        final int block0 = (int) (key >> BLOCK0_SHIFT) & BLOCK0_MASK;
        final int block1 = (int) (key >> BLOCK1_SHIFT) & BLOCK1_MASK;
        final int block2 = (int) (key >> BLOCK2_SHIFT) & BLOCK2_MASK;
        final int indexWithinBlock = (int) (key & INDEX_MASK);

        final short [] blocksInner = ensureBlock(block0, block1, block2);
        final short [] prevBlocksInner = shouldRecordPrevious(key);
        if (prevBlocksInner != null) {
            prevBlocksInner[indexWithinBlock] = blocksInner[indexWithinBlock];
        }
        blocksInner[indexWithinBlock] = value;
    }

    @Override
    public void shift(final RowSet keysToShift, final long shiftDelta) {
        final RowSet.SearchIterator it = (shiftDelta > 0) ? keysToShift.reverseIterator() : keysToShift.searchIterator();
        it.forEachLong((i) -> {
            set(i + shiftDelta, getShort(i));
            setNull(i);
            return true;
        });
    }

    // region boxed methods
    @Override
    public void set(long key, Short value) {
        set(key, unbox(value));
    }

    @Override
    public Short get(long rowKey) {
        return box(getShort(rowKey));
    }

    @Override
    public Short getPrev(long rowKey) {
        return box(getPrevShort(rowKey));
    }
    // endregion boxed methods

    // region primitive get
    @Override
    public final short getShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        return getShortFromBlock(blocks, rowKey);
    }


    @Override
    public final short getPrevShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        if (shouldUsePrevious(rowKey)) {
            return getShortFromBlock(prevBlocks, rowKey);
        }

        return getShortFromBlock(blocks, rowKey);
    }

    private short getShortFromBlock(ShortOneOrN.Block0 blocks, long key) {
        final short [] blocks2 = blocks.getInnermostBlockByKeyOrNull(key);
        if (blocks2 == null) {
            return NULL_SHORT;
        }
        return blocks2[(int)(key & INDEX_MASK)];
    }
    // endregion primitive get

    // region allocateNullFilledBlock
    @SuppressWarnings("SameParameterValue")
    final short [] allocateNullFilledBlock(int size) {
        final short [] newBlock = new short[size];
        Arrays.fill(newBlock, NULL_SHORT);
        return newBlock;
    }
    // endregion allocateNullFilledBlock

    /**
     * Make sure that we have an allocated block at the given point, allocating all of the required parents.
     * @return {@code blocks.get(block0).get(block1).get(block2)}, which is non-null.
     */
    short [] ensureBlock(final int block0, final int block1, final int block2) {
        blocks.ensureIndex(block0, null);
        ShortOneOrN.Block1 blocks0 = blocks.get(block0);
        if (blocks0 == null) {
            blocks.set(block0, blocks0 = new ShortOneOrN.Block1());
        }
        ShortOneOrN.Block2 blocks1 = blocks0.get(block1);
        if (blocks1 == null) {
            blocks0.ensureIndex(block1, null);
            blocks0.set(block1, blocks1 = new ShortOneOrN.Block2());
        }

        short [] result = blocks1.get(block2);
        if (result == null) {
            blocks1.ensureIndex(block2, null);
            // we do not use the recycler here, because the recycler need not sanitize the block (the inUse recycling
            // does that); yet we would like squeaky clean null filled blocks here.
            result = allocateNullFilledBlock(BLOCK_SIZE);
            blocks1.set(block2, result);
        }
        return result;
    }

    /**
     * Make sure that we have an allocated previous and inuse block at the given point, allocating all of the required
     * parents.
     * @return {@code prevBlocks.get(block0).get(block1).get(block2)}, which is non-null.
     */
    private short [] ensurePrevBlock(final long key, final int block0, final int block1, final int block2) {
        if (prevBlocks == null) {
            prevBlocks = new ShortOneOrN.Block0();
            prevInUse = new LongOneOrN.Block0();
        }
        prevBlocks.ensureIndex(block0, recycler0);
        prevInUse.ensureIndex(block0, inUse0Recycler);
        ShortOneOrN.Block1 blocks0 = prevBlocks.get(block0);
        final LongOneOrN.Block1 inUse0;
        if (blocks0 == null) {
            prevBlocks.set(block0, blocks0 = new ShortOneOrN.Block1());
            prevInUse.set(block0, inUse0 = new LongOneOrN.Block1());
        } else {
            inUse0 = prevInUse.get(block0);
        }
        ShortOneOrN.Block2 blocks1 = blocks0.get(block1);
        final LongOneOrN.Block2 inUse1;
        if (blocks1 == null) {
            blocks0.ensureIndex(block1, recycler1);
            inUse0.ensureIndex(block1, inUse1Recycler);
            blocks0.set(block1, blocks1 = new ShortOneOrN.Block2());
            inUse0.set(block1, inUse1 = new LongOneOrN.Block2());
        } else {
            inUse1 = inUse0.get(block1);
        }
        short[] result = blocks1.get(block2);
        if (result == null) {
            blocks1.ensureIndex(block2, recycler2);
            inUse1.ensureIndex(block2, inUse2Recycler);

            blocks1.set(block2, result = recycler.borrowItem());
            inUse1.set(block2, inUseRecycler.borrowItem());

            blocksToFlush.add(key >> BLOCK2_SHIFT);
        }
        return result;
    }

    private boolean shouldTrackPrevious() {
        // prevFlusher == null means we are not tracking previous values yet (or maybe ever).
        // If prepareForParallelPopulation was called on this cycle, it's assumed that all previous values have already
        // been recorded.
        return prevFlusher != null && prepareForParallelPopulationClockCycle != LogicalClock.DEFAULT.currentStep();
    }

    @Override
    public void startTrackingPrevValues() {
        if (prevFlusher != null) {
            throw new IllegalStateException("Can't call startTrackingPrevValues() twice: " +
                    this.getClass().getCanonicalName());
        }
        prevFlusher = new UpdateCommitter<>(this, ShortSparseArraySource::commitUpdates);
    }

    private void commitUpdates() {
        blocksToFlush.sort();

        int destinationOffset = 0;
        long lastBlock2Key = -1;

        final ShortOneOrN.Block0 localPrevBlocks = prevBlocks;
        final LongOneOrN.Block0 localPrevInUse = prevInUse;

        if (localPrevBlocks == null) {
            assert prevInUse == null;
            return;
        }

        // there is no reason to allow these to be used anymore; instead we just null them out so that any
        // getPrev calls will immediately return get().
        prevInUse = null;
        prevBlocks = null;

        // we are clearing out values from block0, block1, block2, block
        // we are accumulating values of block0, block1, block2
        for (int ii = 0; ii < blocksToFlush.size(); ii++) {
            // blockKey = block0 | block1 | block2
            final long blockKey = blocksToFlush.getQuick(ii);
            final long key = blockKey << LOG_BLOCK_SIZE;
            final long block2key = key >> BLOCK1_SHIFT;
            if (block2key != lastBlock2Key) {
                blocksToFlush.set(destinationOffset++, block2key);
                lastBlock2Key = block2key;
            }

            final int block0 = (int) (key >> BLOCK0_SHIFT) & BLOCK0_MASK;
            final int block1 = (int) (key >> BLOCK1_SHIFT) & BLOCK1_MASK;
            final int block2 = (int) (key >> BLOCK2_SHIFT) & BLOCK2_MASK;

            final ShortOneOrN.Block2 blocks1 = localPrevBlocks.get(block0).get(block1);
            final LongOneOrN.Block2 inUse1 = localPrevInUse.get(block0).get(block1);
            final short [] pb = blocks1.get(block2);
            final long[] inuse = inUse1.get(block2);

            inUse1.set(block2, null);
            blocks1.set(block2, null);

            recycler.returnItem(pb);
            inUseRecycler.returnItem(inuse);
        }

        blocksToFlush.remove(destinationOffset, blocksToFlush.size() - destinationOffset);
        destinationOffset = 0;
        long lastBlock1key = -1;

        // we are clearing out values from block0, block1, block2
        // we are accumulating values of block0, block1
        for (int ii = 0; ii < blocksToFlush.size(); ii++) {
            final long blockKey = blocksToFlush.getQuick(ii);
            // blockKey = block0 | block1
            final long key = blockKey << BLOCK1_SHIFT;
            final long block1Key = key >> BLOCK0_SHIFT;

            if (block1Key != lastBlock1key) {
                blocksToFlush.set(destinationOffset++, block1Key);
                lastBlock1key = block1Key;
            }

            final int block0 = (int) (key >> BLOCK0_SHIFT) & BLOCK0_MASK;
            final int block1 = (int) (key >> BLOCK1_SHIFT) & BLOCK1_MASK;

            final ShortOneOrN.Block1 blocks0 = localPrevBlocks.get(block0);
            final LongOneOrN.Block1 prevs0 = localPrevInUse.get(block0);
            final ShortOneOrN.Block2 pb2 = blocks0.get(block1);
            final LongOneOrN.Block2 inuse = prevs0.get(block1);

            prevs0.set(block1, null);
            blocks0.set(block1, null);

            pb2.maybeRecycle(recycler2);
            inuse.maybeRecycle(inUse2Recycler);
        }

        blocksToFlush.remove(destinationOffset, blocksToFlush.size() - destinationOffset);

        // we are clearing out values from block0, block1
        for (int ii = 0; ii < blocksToFlush.size(); ii++) {
            final int block0 = (int) (blocksToFlush.getQuick(ii)) & BLOCK0_MASK;
            final ShortOneOrN.Block1 pb1 = localPrevBlocks.get(block0);
            final LongOneOrN.Block1 inuse = localPrevInUse.get(block0);

            pb1.maybeRecycle(recycler1);
            inuse.maybeRecycle(inUse1Recycler);

            localPrevInUse.set(block0, null);
            localPrevBlocks.set(block0, null);
        }

        blocksToFlush.clear();

        // and finally recycle the top level block of blocks of blocks of blocks
        localPrevBlocks.maybeRecycle(recycler0);
        localPrevInUse.maybeRecycle(inUse0Recycler);
    }

    /**
    * Decides whether to record the previous value.
    * @param key the row key to record
    * @return If the caller should record the previous value, returns prev inner block, the value
    * {@code prevBlocks.get(block0).get(block1).get(block2)}, which is non-null. Otherwise (if the caller should not
     * record values), returns null.
    */
    final short [] shouldRecordPrevious(final long key) {
        if (!shouldTrackPrevious()) {
            return null;
        }
        // If we want to track previous values, we make sure we are registered with the UpdateGraphProcessor.
        prevFlusher.maybeActivate();

        final int block0 = (int) (key >> BLOCK0_SHIFT) & BLOCK0_MASK;
        final int block1 = (int) (key >> BLOCK1_SHIFT) & BLOCK1_MASK;
        final int block2 = (int) (key >> BLOCK2_SHIFT) & BLOCK2_MASK;

        final int indexWithinBlock = (int) (key & INDEX_MASK);
        final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
        final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

        final short[] prevBlockInner = ensurePrevBlock(key, block0, block1, block2);
        final long[] inUse = prevInUse.get(block0).get(block1).get(block2);

        // Set value only if not already in use
        if ((inUse[indexWithinInUse] & maskWithinInUse) == 0) {
            inUse[indexWithinInUse] |= maskWithinInUse;
            return prevBlockInner;
        }
        return null;
    }

    @Override
    public void prepareForParallelPopulation(final RowSequence changedRows) {
        final long currentStep = LogicalClock.DEFAULT.currentStep();
        if (prepareForParallelPopulationClockCycle == currentStep) {
            throw new IllegalStateException("May not call prepareForParallelPopulation twice on one clock cycle!");
        }
        prepareForParallelPopulationClockCycle = currentStep;

        if (changedRows.isEmpty()) {
            return;
        }

        if (prevFlusher != null) {
            prevFlusher.maybeActivate();
        }

        try (final RowSequence.Iterator it = changedRows.getRowSequenceIterator()) {
            do {
                final long firstKey = it.peekNextKey();
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

                final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;
                final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;
                final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;
                final short[] block = ensureBlock(block0, block1, block2);

                if (prevFlusher == null) {
                    it.advance(maxKeyInCurrentBlock + 1);
                    continue;
                }

                final short[] prevBlock = ensurePrevBlock(firstKey, block0, block1, block2);
                final long[] inUse = prevInUse.get(block0).get(block1).get(block2);
                assert inUse != null;

                it.getNextRowSequenceThrough(maxKeyInCurrentBlock).forAllRowKeys(key -> {
                    final int indexWithinBlock = (int) (key & INDEX_MASK);
                    final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                    final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                    prevBlock[indexWithinBlock] = block[indexWithinBlock];
                    inUse[indexWithinInUse] |= maskWithinInUse;
                });
            } while (it.hasMore());
        }
    }

    /**
     * This method supports the 'getPrev' method for its inheritors, doing some of the 'inUse' housekeeping that is
     * common to all inheritors.
     * @return true if the inheritor should return a value from its "prev" data structure; false if it should return a
     * value from its "current" data structure.
     */
    private boolean shouldUsePrevious(final long rowKey) {
        if (prevFlusher == null) {
            return false;
        }

        if (prevInUse == null) {
            return false;
        }

        final long [] inUse = prevInUse.getInnermostBlockByKeyOrNull(rowKey);
        if (inUse == null) {
            return false;
        }

        final int indexWithinBlock = (int) (rowKey & INDEX_MASK);
        final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
        final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

        return (inUse[indexWithinInUse] & maskWithinInUse) != 0;
    }

    // region fillByRanges
    @Override
    /* TYPE_MIXIN */ void fillByRanges(
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        // region chunkDecl
        final WritableShortChunk<? super Values> chunk = dest.asWritableShortChunk();
        // endregion chunkDecl
        final FillByContext<short[]> ctx = new FillByContext<>();
        rowSequence.forAllRowKeyRanges((long firstKey, final long lastKey) -> {
            if (firstKey > ctx.maxKeyInCurrentBlock) {
                ctx.block = blocks.getInnermostBlockByKeyOrNull(firstKey);
                ctx.maxKeyInCurrentBlock = firstKey | INDEX_MASK;
            }
            while (true) {
                final long rightKeyForThisBlock = Math.min(lastKey, ctx.maxKeyInCurrentBlock);
                final int length = (int) (rightKeyForThisBlock - firstKey + 1);
                if (ctx.block == null) {
                    chunk.fillWithNullValue(ctx.offset, length);
                } else {
                    final int sIndexWithinBlock = (int)(firstKey & INDEX_MASK);
                    // for the benefit of code generation.
                    final int offset = ctx.offset;
                    final short[] block = ctx.block;
                    // region copyFromTypedArray
                    chunk.copyFromTypedArray(block, sIndexWithinBlock, offset, length);
                    // endregion copyFromTypedArray
                }
                ctx.offset += length;
                firstKey += length;
                if (firstKey > lastKey) {
                    break;
                }
                ctx.block = blocks.getInnermostBlockByKeyOrNull(firstKey);
                ctx.maxKeyInCurrentBlock = firstKey | INDEX_MASK;
            }
        });
        dest.setSize(ctx.offset);
    }
    // endregion fillByRanges

    // region fillByKeys
    @Override
    /* TYPE_MIXIN */ void fillByKeys(
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        // region chunkDecl
        final WritableShortChunk<? super Values> chunk = dest.asWritableShortChunk();
        // endregion chunkDecl
        final FillByContext<short[]> ctx = new FillByContext<>();
        rowSequence.forEachRowKey((final long v) -> {
            if (v > ctx.maxKeyInCurrentBlock) {
                ctx.block = blocks.getInnermostBlockByKeyOrNull(v);
                ctx.maxKeyInCurrentBlock = v | INDEX_MASK;
            }
            if (ctx.block == null) {
                chunk.fillWithNullValue(ctx.offset, 1);
            } else {
                // region conversion
                chunk.set(ctx.offset, ctx.block[(int) (v & INDEX_MASK)]);
                // endregion conversion
            }
            ++ctx.offset;
            return true;
        });
        dest.setSize(ctx.offset);
    }
    // endregion fillByKeys

    // region fillByUnRowSequence
    @Override
    /* TYPE_MIXIN */ void fillByUnRowSequence(
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys
            /* CONVERTER */) {
        // region chunkDecl
        final WritableShortChunk<? super Values> chunk = dest.asWritableShortChunk();
        // endregion chunkDecl
        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            if (firstKey == RowSequence.NULL_ROW_KEY) {
                chunk.set(ii++, NULL_SHORT);
                continue;
            }
            final long masked = firstKey & ~INDEX_MASK;
            int lastII = ii;
            while (lastII + 1 < keys.size()) {
                final int nextII = lastII + 1;
                final long nextKey = keys.get(nextII);
                final long nextMasked = nextKey & ~INDEX_MASK;
                if (nextMasked != masked) {
                    break;
                }
                lastII = nextII;
            }
            final short [] block = blocks.getInnermostBlockByKeyOrNull(firstKey);
            if (block == null) {
                chunk.fillWithNullValue(ii, lastII - ii + 1);
                ii = lastII + 1;
                continue;
            }
            while (ii <= lastII) {
                final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);
                // region conversion
                chunk.set(ii++, block[indexWithinBlock]);
                // endregion conversion
            }
        }
        dest.setSize(keys.size());
    }

    @Override
    /* TYPE_MIXIN */ void fillPrevByUnRowSequence(
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys
            /* CONVERTER */) {
        // region chunkDecl
        final WritableShortChunk<? super Values> chunk = dest.asWritableShortChunk();
        // endregion chunkDecl
        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            if (firstKey == RowSequence.NULL_ROW_KEY) {
                chunk.set(ii++, NULL_SHORT);
                continue;
            }
            final long masked = firstKey & ~INDEX_MASK;
            int lastII = ii;
            while (lastII + 1 < keys.size()) {
                final int nextII = lastII + 1;
                final long nextKey = keys.get(nextII);
                final long nextMasked = nextKey & ~INDEX_MASK;
                if (nextMasked != masked) {
                    break;
                }
                lastII = nextII;
            }

            final short [] block = blocks.getInnermostBlockByKeyOrNull(firstKey);
            if (block == null) {
                chunk.fillWithNullValue(ii, lastII - ii + 1);
                ii = lastII + 1;
                continue;
            }

            final long [] prevInUse = (prevFlusher == null || this.prevInUse == null) ? null : this.prevInUse.getInnermostBlockByKeyOrNull(firstKey);
            final short [] prevBlock = prevInUse == null ? null : prevBlocks.getInnermostBlockByKeyOrNull(firstKey);
            while (ii <= lastII) {
                final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);
                final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                final short[] blockToUse = (prevInUse != null && (prevInUse[indexWithinInUse] & maskWithinInUse) != 0) ? prevBlock : block;
                // region conversion
                chunk.set(ii++, blockToUse == null ? NULL_SHORT : blockToUse[indexWithinBlock]);
                // endregion conversion
            }
        }
        dest.setSize(keys.size());
    }
    // endregion fillByUnRowSequence

    // region fillFromChunkByRanges
    @Override
    /* TYPE_MIXIN */ void fillFromChunkByRanges(
            @NotNull final RowSequence rowSequence,
            @NotNull final Chunk<? extends Values> src
            /* CONVERTER */) {
        if (rowSequence.isEmpty()) {
            return;
        }
        // region chunkDecl
        final ShortChunk<? extends Values> chunk = src.asShortChunk();
        // endregion chunkDecl
        final LongChunk<OrderedRowKeyRanges> ranges = rowSequence.asRowKeyRangesChunk();

        final boolean trackPrevious = shouldTrackPrevious();

        if (trackPrevious) {
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

                final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;
                final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;
                final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;
                final short [] block = ensureBlock(block0, block1, block2);

                if (block != knownUnaliasedBlock && chunk.isAlias(block)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
                knownUnaliasedBlock = block;

                final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
                // This 'if' with its constant condition should be very friendly to the branch predictor.
                if (trackPrevious) {
                    final short[] prevBlock = ensurePrevBlock(firstKey, block0, block1, block2);
                    final long[] inUse = prevInUse.get(block0).get(block1).get(block2);

                    assert inUse != null;
                    assert prevBlock != null;

                    for (int jj = 0; jj < length; ++jj) {
                        final int indexWithinBlock = sIndexWithinBlock + jj;
                        final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                        final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                        if ((inUse[indexWithinInUse] & maskWithinInUse) == 0) {
                            prevBlock[indexWithinBlock] = block[indexWithinBlock];
                            inUse[indexWithinInUse] |= maskWithinInUse;
                        }
                    }
                }

                // region copyToTypedArray
                chunk.copyToTypedArray(offset, block, sIndexWithinBlock, length);
                // endregion copyToTypedArray

                firstKey += length;
                offset += length;
            }
        }
    }
    // endregion fillFromChunkByRanges

    // region fillFromChunkByKeys
    @Override
    /* TYPE_MIXIN */ void fillFromChunkByKeys(
            @NotNull final RowSequence rowSequence,
            @NotNull final Chunk<? extends Values> src
            /* CONVERTER */) {
        if (rowSequence.isEmpty()) {
            return;
        }
        // region chunkDecl
        final ShortChunk<? extends Values> chunk = src.asShortChunk();
        // endregion chunkDecl
        final LongChunk<OrderedRowKeys> keys = rowSequence.asRowKeyChunk();

        final boolean trackPrevious = shouldTrackPrevious();;

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

            final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;
            final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;
            final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;
            final short [] block = ensureBlock(block0, block1, block2);

            if (chunk.isAlias(block)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            // This conditional with its constant condition should be very friendly to the branch predictor.
            final short[] prevBlock = trackPrevious ? ensurePrevBlock(firstKey, block0, block1, block2) : null;
            final long[] inUse = trackPrevious ? prevInUse.get(block0).get(block1).get(block2) : null;

            while (ii <= lastII) {
                final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);
                // This 'if' with its constant condition should be very friendly to the branch predictor.
                if (trackPrevious) {
                    assert inUse != null;
                    assert prevBlock != null;

                    final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                    final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                    if ((inUse[indexWithinInUse] & maskWithinInUse) == 0) {
                        prevBlock[indexWithinBlock] = block[indexWithinBlock];
                        inUse[indexWithinInUse] |= maskWithinInUse;
                    }
                }
                // region conversion
                block[indexWithinBlock] = chunk.get(ii);
                // endregion conversion
                ++ii;
            }
        }
    }
    // endregion fillFromChunkByKeys

    // region nullByRanges
    @Override
    void nullByRanges(@NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return;
        }

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        try (RowSequence.Iterator okIt = rowSequence.getRowSequenceIterator()) {
            while (okIt.hasMore()) {
                final long firstKey = okIt.peekNextKey();
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
                final RowSequence blockOk = okIt.getNextRowSequenceThrough(maxKeyInCurrentBlock);

                final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;
                final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;
                final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;
                final short [] block = blocks.getInnermostBlockByKeyOrNull(firstKey);

                if (block == null) {
                    continue;
                }

                blockOk.forAllRowKeyRanges((s, e) -> {
                    final int length = (int)((e - s) + 1);

                    final int sIndexWithinBlock = (int) (s & INDEX_MASK);
                    // This 'if' with its constant condition should be very friendly to the branch predictor.
                    if (hasPrev) {
                        boolean prevRequired = false;
                        for (int jj = 0; jj < length; ++jj) {
                            final int indexWithinBlock = sIndexWithinBlock + jj;
                            if (block[indexWithinBlock] != NULL_SHORT) {
                                prevRequired = true;
                                break;
                            }
                        }

                        if (prevRequired) {
                            final short[] prevBlock = ensurePrevBlock(firstKey, block0, block1, block2);
                            final long[] inUse = prevInUse.get(block0).get(block1).get(block2);

                            assert inUse != null;
                            assert prevBlock != null;

                            for (int jj = 0; jj < length; ++jj) {
                                final int indexWithinBlock = sIndexWithinBlock + jj;
                                final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                                final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                                if ((inUse[indexWithinInUse] & maskWithinInUse) == 0) {
                                    prevBlock[indexWithinBlock] = block[indexWithinBlock];
                                    inUse[indexWithinInUse] |= maskWithinInUse;
                                }
                            }

                            Arrays.fill(block, sIndexWithinBlock, sIndexWithinBlock + length, NULL_SHORT);
                        }
                    } else {
                        Arrays.fill(block, sIndexWithinBlock, sIndexWithinBlock + length, NULL_SHORT);
                    }
                });
            }
        }
    }
    // endregion nullByRanges

    // region nullByKeys
    @Override
    void nullByKeys(@NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return;
        }

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        try (RowSequence.Iterator okIt = rowSequence.getRowSequenceIterator()) {
            while (okIt.hasMore()) {
                final long firstKey = okIt.peekNextKey();
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
                final RowSequence blockOk = okIt.getNextRowSequenceThrough(maxKeyInCurrentBlock);

                final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;
                final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;
                final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;
                final short[] block = blocks.getInnermostBlockByKeyOrNull(firstKey);
                if (block == null) {
                    continue;
                }

                MutableObject<short[]> prevBlock = new MutableObject<>();
                MutableObject<long[]> inUse = new MutableObject<>();

                blockOk.forAllRowKeys(key -> {

                    final int indexWithinBlock = (int) (key & INDEX_MASK);
                    // This 'if' with its constant condition should be very friendly to the branch predictor.
                    if (hasPrev) {

                        final short oldValue = block[indexWithinBlock];
                        if (oldValue != NULL_SHORT) {
                            if (prevBlock.getValue() == null) {
                                prevBlock.setValue(ensurePrevBlock(firstKey, block0, block1, block2));
                                inUse.setValue(prevInUse.get(block0).get(block1).get(block2));
                            }

                            final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                            final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                            if ((inUse.getValue()[indexWithinInUse] & maskWithinInUse) == 0) {
                                prevBlock.getValue()[indexWithinBlock] = oldValue;
                                inUse.getValue()[indexWithinInUse] |= maskWithinInUse;
                            }
                        }
                    }
                    block[indexWithinBlock] = NULL_SHORT;
                });
            }
        }
    }
    // endregion nullByKeys

    // region fillFromChunkUnordered
    @Override
    public /* TYPE_MIXIN */ void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys
            /* CONVERTER */) {
        if (keys.size() == 0) {
            return;
        }
        // region chunkDecl
        final ShortChunk<? extends Values> chunk = src.asShortChunk();
        // endregion chunkDecl

        final boolean trackPrevious = shouldTrackPrevious();;

        if (trackPrevious) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long minKeyInCurrentBlock = firstKey & ~INDEX_MASK;
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

            final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;
            final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;
            final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;
            final short [] block = ensureBlock(block0, block1, block2);

            if (chunk.isAlias(block)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            // This conditional with its constant condition should be very friendly to the branch predictor.
            final short[] prevBlock = trackPrevious ? ensurePrevBlock(firstKey, block0, block1, block2) : null;
            final long[] inUse = trackPrevious ? prevInUse.get(block0).get(block1).get(block2) : null;

            long key = keys.get(ii);
            do {
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (trackPrevious) {
                    assert inUse != null;

                    final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                    final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                    if ((inUse[indexWithinInUse] & maskWithinInUse) == 0) {
                        prevBlock[indexWithinBlock] = block[indexWithinBlock];
                        inUse[indexWithinInUse] |= maskWithinInUse;
                    }
                }
                // region conversion
                block[indexWithinBlock] = chunk.get(ii);
                // endregion conversion
                ++ii;
            } while (ii < keys.size() && (key = keys.get(ii)) >= minKeyInCurrentBlock && key <= maxKeyInCurrentBlock);
        }
    }
    // endregion fillFromChunkUnordered

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final RowSequence rowSequence) {
        if (prevFlusher == null) {
            fillChunk(context, dest, rowSequence);
            return;
        }
        defaultFillPrevChunk(context, dest, rowSequence);
    }

    // region getChunk
    @Override
    public ShortChunk<Values> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return ShortChunk.getEmptyChunk();
        }
        final long firstKey = rowSequence.firstRowKey();
        final long lastKey = rowSequence.lastRowKey();
        if ((lastKey - firstKey + 1) == rowSequence.size() && (firstKey >> BLOCK2_SHIFT == lastKey >> BLOCK2_SHIFT)) {
            // it's a contiguous range, in a single block
            return DefaultGetContext.resetChunkFromArray(context,
                    blocks.getInnermostBlockByKeyOrNull(firstKey),
                    (int) (firstKey & INDEX_MASK),
                    (int) rowSequence.size());
        }
        return getChunkByFilling(context, rowSequence).asShortChunk();
    }
    // endregion getChunk

    // region getPrevChunk
    @Override
    public ShortChunk<Values> getPrevChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (prevFlusher == null) {
            return getChunk(context, rowSequence);
        }
        return getPrevChunkByFilling(context, rowSequence).asShortChunk();
    }
    // endregion getPrevChunk

    // region reinterpretation
    // endregion reinterpretation
}
