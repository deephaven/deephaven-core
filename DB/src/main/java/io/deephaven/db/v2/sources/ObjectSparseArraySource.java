/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterSparseArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.tables.dbarrays.DbArrayBase;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.sparse.ObjectOneOrN;
import io.deephaven.db.v2.sources.sparse.LongOneOrN;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.UpdateCommitter;
import io.deephaven.util.SoftRecycler;
import gnu.trove.list.array.TLongArrayList;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.Arrays;

// region boxing imports
// endregion boxing imports

import static io.deephaven.db.v2.sources.sparse.SparseConstants.*;

/**
 * Sparse array source for Object.
 * <p>
 * The C-haracterSparseArraySource is replicated to all other types with
 * io.deephaven.db.v2.sources.Replicate.
 *
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ObjectSparseArraySource<T> extends SparseArrayColumnSource<T> implements MutableColumnSourceGetDefaults.ForObject<T> {
    // region recyclers
    private static final SoftRecycler recycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new Object[BLOCK_SIZE], block -> Arrays.fill(block, null)); // we'll hold onto previous values, fix that
    private static final SoftRecycler recycler2 = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new Object[BLOCK2_SIZE][], null);
    private static final SoftRecycler recycler1 = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new ObjectOneOrN.Block2[BLOCK1_SIZE], null);
    private static final SoftRecycler recycler0 = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new ObjectOneOrN.Block1[BLOCK0_SIZE], null);
    // endregion recyclers

    /**
     * The presence of a prevFlusher means that this ArraySource wants to track previous values. If prevFlusher is null,
     * the ArraySource does not want (or does not yet want) to track previous values. Deserialized ArraySources never
     * track previous values.
     */
    protected transient UpdateCommitter<ObjectSparseArraySource> prevFlusher = null;

    /**
     * Our previous page table could be very sparse, and we do not want to read through millions of nulls to find out
     * what blocks to recycle.  Instead we maintain a list of blocks that we have allocated (as the key shifted by
     * BLOCK0_SHIFT).  We recycle those blocks in the PrevFlusher; and accumulate the set of blocks that must be
     * recycled from the next level array, and so on until we recycle the top-level prevBlocks and prevInUse arrays.
     */
    private transient final TLongArrayList blocksToFlush = new TLongArrayList();

    protected ObjectOneOrN.Block0<T> blocks;
    protected transient ObjectOneOrN.Block0<T> prevBlocks;

    // region constructor
    private final boolean isArrayType;

    ObjectSparseArraySource(Class<T> type) {
        super(type);
        blocks = new ObjectOneOrN.Block0<>();
        isArrayType = DbArrayBase.class.isAssignableFrom(type);
    }

    ObjectSparseArraySource(Class<T> type, Class componentType) {
        super(type, componentType);
        blocks = new ObjectOneOrN.Block0<>();
        isArrayType = DbArrayBase.class.isAssignableFrom(type);
    }
    // endregion constructor

    // region serialization
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        final Index.SequentialBuilder sb = Index.FACTORY.getSequentialBuilder();
        blocks.enumerate(null, sb::appendKey);
        final Index index = sb.getIndex();

        final int size = index.intSize();
        final T [] data = (T [])new Object[size];
        // noinspection unchecked
        final ColumnSource<T> reinterpreted = (ColumnSource<T>) reinterpretForSerialization();
        try (final FillContext context = reinterpreted.makeFillContext(size);
             final ResettableWritableObjectChunk<T, Values> destChunk = ResettableWritableObjectChunk.makeResettableChunk()) {
            destChunk.resetFromTypedArray(data, 0, size);
            // noinspection unchecked
            reinterpreted.fillChunk(context, destChunk, index);
        }
        out.writeObject(index);
        out.writeObject(data);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        blocks = new ObjectOneOrN.Block0<T>();

        final Index index = (Index)in.readObject();
        final T [] data = (T [])in.readObject();
        final ObjectChunk<T, Values> srcChunk = ObjectChunk.chunkWrap(data);
        // noinspection unchecked
        final WritableSource<Object> reinterpreted = (WritableSource<Object>) reinterpretForSerialization();
        try (final FillFromContext context = reinterpreted.makeFillFromContext(index.intSize())) {
            reinterpreted.fillFromChunk(context, srcChunk, index);
        }
    }
    // endregion serialization

    private void readObjectNoData() throws ObjectStreamException {
        throw new StreamCorruptedException();
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        // Nothing to do here. Sparse array sources allocate on-demand and always null-fill.
    }

    @Override
    public final void set(long key, T value) {
        final int block0 = (int) (key >> BLOCK0_SHIFT) & BLOCK0_MASK;
        final int block1 = (int) (key >> BLOCK1_SHIFT) & BLOCK1_MASK;
        final int block2 = (int) (key >> BLOCK2_SHIFT) & BLOCK2_MASK;
        final int indexWithinBlock = (int) (key & INDEX_MASK);

        final T [] blocksInner = ensureBlock(block0, block1, block2);
        final T [] prevBlocksInner = shouldRecordPrevious(key);
        if (prevBlocksInner != null) {
            prevBlocksInner[indexWithinBlock] = blocksInner[indexWithinBlock];
        }
        blocksInner[indexWithinBlock] = value;
    }

    @Override
    public void shift(final Index keysToShift, final long shiftDelta) {
        final Index.SearchIterator it = (shiftDelta > 0) ? keysToShift.reverseIterator() : keysToShift.searchIterator();
        it.forEachLong((i) -> {
            set(i + shiftDelta, get(i));
            set(i, null);
            return true;
        });
    }

    @Override
    public void remove(Index toRemove) {
        toRemove.forEachLong((i) -> { set(i, null); return true; });
    }

    // region boxed methods
    // endregion boxed methods

    // region copy method
    @Override
    public void copy(ColumnSource<? extends T> sourceColumn, long sourceKey, long destKey) {
        final T value = sourceColumn.get(sourceKey);

        if (isArrayType && value instanceof DbArrayBase) {
            final DbArrayBase<?> dbArray = (DbArrayBase<?>) value;
            // noinspection unchecked
            set(destKey, (T) dbArray.getDirect());
        } else {
            set(destKey, value);
        }
    }
    // endregion copy method

    // region primitive get
    @Override
    public final T get(long index) {
        if (index < 0) {
            return null;
        }
        return getFromBlock(blocks, index);
    }


    @Override
    public final T getPrev(long index) {
        if (index < 0) {
            return null;
        }
        if (shouldUsePrevious(index)) {
            return getFromBlock(prevBlocks, index);
        }

        return getFromBlock(blocks, index);
    }

    private T getFromBlock(ObjectOneOrN.Block0<T> blocks, long key) {
        final T [] blocks2 = blocks.getInnermostBlockByKeyOrNull(key);
        if (blocks2 == null) {
            return null;
        }
        return blocks2[(int)(key & INDEX_MASK)];
    }
    // endregion primitive get

    // region allocateNullFilledBlock
    final T[] allocateNullFilledBlock(int size){
        //noinspection unchecked
        return (T[]) new Object[size];
    }
    // endregion allocateNullFilledBlock

    /**
     * Make sure that we have an allocated block at the given point, allocating all of the required parents.
     * @return {@code blocks.get(block0).get(block1).get(block2)}, which is non-null.
     */
    T [] ensureBlock(final int block0, final int block1, final int block2) {
        blocks.ensureIndex(block0, null);
        ObjectOneOrN.Block1<T> blocks0 = blocks.get(block0);
        if (blocks0 == null) {
            blocks.set(block0, blocks0 = new ObjectOneOrN.Block1<T>());
        }
        ObjectOneOrN.Block2<T> blocks1 = blocks0.get(block1);
        if (blocks1 == null) {
            blocks0.ensureIndex(block1, null);
            blocks0.set(block1, blocks1 = new ObjectOneOrN.Block2<T>());
        }

        T [] result = blocks1.get(block2);
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
    private T [] ensurePrevBlock(final long key, final int block0, final int block1, final int block2) {
        if (prevBlocks == null) {
            prevBlocks = new ObjectOneOrN.Block0<T>();
            prevInUse = new LongOneOrN.Block0();
        }
        prevBlocks.ensureIndex(block0, recycler0);
        prevInUse.ensureIndex(block0, inUse0Recycler);
        ObjectOneOrN.Block1<T> blocks0 = prevBlocks.get(block0);
        final LongOneOrN.Block1 inUse0;
        if (blocks0 == null) {
            prevBlocks.set(block0, blocks0 = new ObjectOneOrN.Block1<T>());
            prevInUse.set(block0, inUse0 = new LongOneOrN.Block1());
        } else {
            inUse0 = prevInUse.get(block0);
        }
        ObjectOneOrN.Block2<T> blocks1 = blocks0.get(block1);
        final LongOneOrN.Block2 inUse1;
        if (blocks1 == null) {
            blocks0.ensureIndex(block1, recycler1);
            inUse0.ensureIndex(block1, inUse1Recycler);
            blocks0.set(block1, blocks1 = new ObjectOneOrN.Block2<T>());
            inUse0.set(block1, inUse1 = new LongOneOrN.Block2());
        } else {
            inUse1 = inUse0.get(block1);
        }
        T [] result = blocks1.get(block2);
        if (result == null) {
            blocks1.ensureIndex(block2, recycler2);
            inUse1.ensureIndex(block2, inUse2Recycler);

            blocks1.set(block2, result = (T[])recycler.borrowItem());
            inUse1.set(block2, inUseRecycler.borrowItem());

            blocksToFlush.add(key >> BLOCK2_SHIFT);
        }
        return result;
    }

    @Override
    public void startTrackingPrevValues() {
        if (prevFlusher != null) {
            throw new IllegalStateException("Can't call startTrackingPrevValues() twice: " +
                    this.getClass().getCanonicalName());
        }
        prevFlusher = new UpdateCommitter<>(this, ObjectSparseArraySource::commitUpdates);
    }

    private void commitUpdates() {
        blocksToFlush.sort();

        int destinationOffset = 0;
        long lastBlock2Key = -1;

        final ObjectOneOrN.Block0<T> localPrevBlocks = prevBlocks;
        final LongOneOrN.Block0 localPrevInUse = prevInUse;

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

            final ObjectOneOrN.Block2<T> blocks1 = localPrevBlocks.get(block0).get(block1);
            final LongOneOrN.Block2 inUse1 = localPrevInUse.get(block0).get(block1);
            final T [] pb = blocks1.get(block2);
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

            final ObjectOneOrN.Block1<T> blocks0 = localPrevBlocks.get(block0);
            final LongOneOrN.Block1 prevs0 = localPrevInUse.get(block0);
            final ObjectOneOrN.Block2<T> pb2 = blocks0.get(block1);
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
            final ObjectOneOrN.Block1<T> pb1 = localPrevBlocks.get(block0);
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
    * @param key the index to record
    * @return If the caller should record the previous value, returns prev inner block, the value
    * {@code prevBlocks.get(block0).get(block1).get(block2)}, which is non-null. Otherwise (if the caller should not
     * record values), returns null.
    */
    final T [] shouldRecordPrevious(final long key) {
        // prevFlusher == null means we are not tracking previous values yet (or maybe ever)
        if (prevFlusher == null) {
            return null;
        }
        // If we want to track previous values, we make sure we are registered with the LiveTableMonitor.
        prevFlusher.maybeActivate();

        final int block0 = (int) (key >> BLOCK0_SHIFT) & BLOCK0_MASK;
        final int block1 = (int) (key >> BLOCK1_SHIFT) & BLOCK1_MASK;
        final int block2 = (int) (key >> BLOCK2_SHIFT) & BLOCK2_MASK;

        final int indexWithinBlock = (int) (key & INDEX_MASK);
        final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
        final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

        final T [] prevBlockInner = ensurePrevBlock(key, block0, block1, block2);
        final long[] inUse = prevInUse.get(block0).get(block1).get(block2);

        // Set value only if not already in use
        if ((inUse[indexWithinInUse] & maskWithinInUse) == 0) {
            inUse[indexWithinInUse] |= maskWithinInUse;
            return prevBlockInner;
        }
        return null;
    }

    /**
     * This method supports the 'getPrev' method for its inheritors, doing some of the 'inUse' housekeeping that is
     * common to all inheritors.
     * @return true if the inheritor should return a value from its "prev" data structure; false if it should return a
     * value from its "current" data structure.
     */
    private boolean shouldUsePrevious(final long index) {
        if (prevFlusher == null) {
            return false;
        }

        if (prevInUse == null) {
            return false;
        }

        final long [] inUse = prevInUse.getInnermostBlockByKeyOrNull(index);
        if (inUse == null) {
            return false;
        }

        final int indexWithinBlock = (int) (index & INDEX_MASK);
        final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
        final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

        return (inUse[indexWithinInUse] & maskWithinInUse) != 0;
    }

    // region fillByRanges
    @Override
    void fillByRanges(@NotNull WritableChunk<? super Values> dest, @NotNull OrderedKeys orderedKeys) {
        final WritableObjectChunk<T, ? super Values> chunk = dest.asWritableObjectChunk();
        final FillByContext<T []> ctx = new FillByContext<>();
        orderedKeys.forAllLongRanges((long firstKey, final long lastKey) -> {
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
                    final T [] block = ctx.block;
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
    void fillByKeys(@NotNull WritableChunk<? super Values> dest, @NotNull OrderedKeys orderedKeys) {
        final WritableObjectChunk<T, ? super Values> chunk = dest.asWritableObjectChunk();
        final FillByContext<T []> ctx = new FillByContext<>();
        orderedKeys.forEachLong((final long v) -> {
            if (v > ctx.maxKeyInCurrentBlock) {
                ctx.block = blocks.getInnermostBlockByKeyOrNull(v);
                ctx.maxKeyInCurrentBlock = v | INDEX_MASK;
            }
            if (ctx.block == null) {
                chunk.fillWithNullValue(ctx.offset, 1);
            } else {
                chunk.set(ctx.offset, ctx.block[(int) (v & INDEX_MASK)]);
            }
            ++ctx.offset;
            return true;
        });
        dest.setSize(ctx.offset);
    }
    // endregion fillByKeys

    // region fillByUnorderedKeys
    @Override
    void fillByUnorderedKeys(@NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends KeyIndices> keys) {
        final WritableObjectChunk<T, ? super Values> ObjectChunk = dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            if (firstKey == Index.NULL_KEY) {
                ObjectChunk.set(ii++, null);
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
            final T [] block = blocks.getInnermostBlockByKeyOrNull(firstKey);
            if (block == null) {
                ObjectChunk.fillWithNullValue(ii, lastII - ii + 1);
                ii = lastII + 1;
                continue;
            }
            while (ii <= lastII) {
                final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);
                ObjectChunk.set(ii++, block[indexWithinBlock]);
            }
        }
        dest.setSize(keys.size());
    }

    @Override
    void fillPrevByUnorderedKeys(@NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends KeyIndices> keys) {
        final WritableObjectChunk<T, ? super Values> ObjectChunk = dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            if (firstKey == Index.NULL_KEY) {
                ObjectChunk.set(ii++, null);
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

            final T [] block = blocks.getInnermostBlockByKeyOrNull(firstKey);
            if (block == null) {
                ObjectChunk.fillWithNullValue(ii, lastII - ii + 1);
                ii = lastII + 1;
                continue;
            }

            final long [] prevInUse = (prevFlusher == null || this.prevInUse == null) ? null : this.prevInUse.getInnermostBlockByKeyOrNull(firstKey);
            final T [] prevBlock = prevInUse == null ? null : prevBlocks.getInnermostBlockByKeyOrNull(firstKey);
            while (ii <= lastII) {
                final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);
                final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                final T [] blockToUse = (prevInUse != null && (prevInUse[indexWithinInUse] & maskWithinInUse) != 0) ? prevBlock : block;
                ObjectChunk.set(ii++, blockToUse == null ? null : blockToUse[indexWithinBlock]);
            }
        }
        dest.setSize(keys.size());
    }
    // endregion fillByUnorderedKeys

    // region fillFromChunkByRanges
    @Override
    void fillFromChunkByRanges(@NotNull OrderedKeys orderedKeys, Chunk<? extends Values> src) {
        if (orderedKeys.size() == 0) {
            return;
        }
        final ObjectChunk<T, ? extends Values> chunk = src.asObjectChunk();
        final LongChunk<OrderedKeyRanges> ranges = orderedKeys.asKeyRangesChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        int offset = 0;
        // This helps us reduce the number of calls to Chunk.isAlias
        T [] knownUnaliasedBlock = null;
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
                final T [] block = ensureBlock(block0, block1, block2);

                if (block != knownUnaliasedBlock && chunk.isAlias(block)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
                knownUnaliasedBlock = block;

                final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
                // This 'if' with its constant condition should be very friendly to the branch predictor.
                if (hasPrev) {
                    final T [] prevBlock = ensurePrevBlock(firstKey, block0, block1, block2);
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
    void fillFromChunkByKeys(@NotNull OrderedKeys orderedKeys, Chunk<? extends Values> src) {
        if (orderedKeys.size() == 0) {
            return;
        }
        final ObjectChunk<T, ? extends Values> chunk = src.asObjectChunk();
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

            final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;
            final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;
            final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;
            final T [] block = ensureBlock(block0, block1, block2);

            if (chunk.isAlias(block)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            // This conditional with its constant condition should be very friendly to the branch predictor.
            final T [] prevBlock = hasPrev ? ensurePrevBlock(firstKey, block0, block1, block2) : null;
            final long[] inUse = hasPrev ? prevInUse.get(block0).get(block1).get(block2) : null;

            while (ii <= lastII) {
                final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);
                // This 'if' with its constant condition should be very friendly to the branch predictor.
                if (hasPrev) {
                    assert inUse != null;
                    assert prevBlock != null;

                    final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                    final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                    if ((inUse[indexWithinInUse] & maskWithinInUse) == 0) {
                        prevBlock[indexWithinBlock] = block[indexWithinBlock];
                        inUse[indexWithinInUse] |= maskWithinInUse;
                    }
                }
                block[indexWithinBlock] = chunk.get(ii);
                ++ii;
            }
        }
    }
    // endregion fillFromChunkByKeys

    // region fillFromChunkUnordered
    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<KeyIndices> keys) {
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

            final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;
            final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;
            final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;
            final T [] block = ensureBlock(block0, block1, block2);

            if (chunk.isAlias(block)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            // This conditional with its constant condition should be very friendly to the branch predictor.
            final T [] prevBlock = hasPrev ? ensurePrevBlock(firstKey, block0, block1, block2) : null;
            final long[] inUse = hasPrev ? prevInUse.get(block0).get(block1).get(block2) : null;

            long key = keys.get(ii);
            do {
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (hasPrev) {
                    assert inUse != null;

                    final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                    final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                    if ((inUse[indexWithinInUse] & maskWithinInUse) == 0) {
                        prevBlock[indexWithinBlock] = block[indexWithinBlock];
                        inUse[indexWithinInUse] |= maskWithinInUse;
                    }
                }
                block[indexWithinBlock] = chunk.get(ii);
                ++ii;
            } while (ii < keys.size() && (key = keys.get(ii)) >= minKeyInCurrentBlock && key <= maxKeyInCurrentBlock);
        }
    }
    // endregion fillFromChunkUnordered

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull OrderedKeys orderedKeys) {
        if (prevFlusher == null) {
            fillChunk(context, dest, orderedKeys);
            return;
        }
        defaultFillPrevChunk(context, dest, orderedKeys);
    }

    // region getChunk
    @Override
    public ObjectChunk<T, Values> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        if (orderedKeys.size() == 0) {
            return ObjectChunk.getEmptyChunk();
        }
        final long firstKey = orderedKeys.firstKey();
        final long lastKey = orderedKeys.lastKey();
        if ((lastKey - firstKey + 1) == orderedKeys.size() && (firstKey >> BLOCK2_SHIFT == lastKey >> BLOCK2_SHIFT)) {
            // it's a contiguous range, in a single block
            return DefaultGetContext.resetChunkFromArray(context,
                    blocks.getInnermostBlockByKeyOrNull(firstKey),
                    (int) (firstKey & INDEX_MASK),
                    (int)orderedKeys.size());
        }
        return getChunkByFilling(context, orderedKeys).asObjectChunk();
    }
    // endregion getChunk

    // region getPrevChunk
    @Override
    public ObjectChunk<T, Values> getPrevChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        if (prevFlusher == null) {
            return getChunk(context, orderedKeys);
        }
        return getPrevChunkByFilling(context, orderedKeys).asObjectChunk();
    }
    // endregion getPrevChunk

    // region reinterpretation
    // endregion reinterpretation
}
