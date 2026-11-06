//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.sources.sparse.LongOneOrN;
import io.deephaven.util.SoftRecycler;
import io.deephaven.util.SoftRecyclerWindow;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.*;

/**
 * A column source backed by arrays that may not be filled in all blocks.
 *
 * <p>
 * To store the blocks, we use a multi-level page table like structure. Each entry that exists is complete, i.e. we
 * never reallocate partial blocks, we always allocate the complete block. The row key is divided as follows:
 * </p>
 * <table>
 * <tr>
 * <th>Description</td>
 * <th>Size</th>
 * <th>Bits</th>
 * </tr>
 * <tr>
 * <td>Block 0</td>
 * <td>19</td>
 * <td>62-44</td>
 * </tr>
 * <tr>
 * <td>Block 1</td>
 * <td>18</td>
 * <td>43-26</td>
 * </tr>
 * <tr>
 * <td>Block 2</td>
 * <td>18</td>
 * <td>25-8</td>
 * </tr>
 * <tr>
 * <td>Index Within Block</td>
 * <td>8</td>
 * <td>7-0</td>
 * </tr>
 * </table>
 * <p>
 * Bit 63, the sign bit, is used to indicate null (that is, all negative numbers are defined to be null)
 * </p>
 * <p>
 * Parallel structures are used for previous values and prevInUse. We recycle all levels of the previous blocks, so that
 * the previous structure takes up memory only while it is in use.
 * </p>
 */
public abstract class OneOrNSparseArrayColumnSource<T> extends SparseArrayColumnSource<T> {

    // Usage:
    //
    // To access a "current" data element:
    // final int block0 = (int) (key >> (LOG_BLOCK_SIZE + LOG_BLOCK1_SIZE + LOG_BLOCK2_SIZE))
    // final int block1 = (int) (key >> (LOG_BLOCK_SIZE + LOG_BLOCK1_SIZE))
    // final int block2 = (int) (key >> (LOG_BLOCK_SIZE))
    // final int indexWithinBlock = (int) (key & INDEX_MASK);
    // data = blocks[block0][block1][block2][indexWithinBlock];
    //
    // To access a "previous" data element: the structure is identical, except you refer to the prev structure:
    // prevData = prevBlocks[block0][block1][block2][indexWithinBlock];
    //
    // To access a true/false entry from the "prevInUse" data structure: the structure is similar, except that the
    // innermost array is logically is a two-level structure: it is an array of "bitsets", where each "bitset" is a
    // 64-element "array" of bits, in reality a 64-bit long. If we were able to access the bitset as an array, the code
    // would be:
    // bool inUse = prevInUse[block0][block1][block2][indexWithinInUse][inUseBitIndex]
    // The actual code is:
    // bool inUse = (prevInUse[block0][block1][block2][indexWithinInUse] & maskWithinInUse) != 0
    //
    // Where:
    // indexWithinInUse = indexWithinBlock / 64
    // inUseBitIndex = indexWithinBlock % 64
    // maskWithinInUse = 1L << inUseBitIndex
    //
    // and, if an inUse block is null (at any level), then the inUse result is defined as false.
    //
    // In the code below we do all the calculations in the "log" space so, in actuality it's more like
    // indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
    // maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);
    //
    // Finally, this bitset manipulation logic only really makes sense if the innermost data block size is larger than
    // the bitset size (64), so we have the additional constraint that LOG_BLOCK_SIZE >= LOG_INUSE_BITSET_SIZE.

    static {
        Assert.geq(LOG_BLOCK_SIZE, "LOG_BLOCK_SIZE", LOG_INUSE_BITSET_SIZE);
    }

    // the lowest level inUse bitmap recycle
    static final SoftRecycler<long[]> inUseRecycler =
            SoftRecyclerWindow
                    .register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_CAPACITY,
                            SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_MAXIMUM_CAPACITY,
                            () -> new long[IN_USE_BLOCK_SIZE],
                            block -> Arrays.fill(block, 0)));

    // the recycler for blocks of bitmaps
    static final SoftRecycler<long[][]> inUse2Recycler =
            SoftRecyclerWindow
                    .register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_CAPACITY2,
                            SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_MAXIMUM_CAPACITY2,
                            () -> new long[BLOCK2_SIZE][],
                            null));

    // the recycler for blocks of blocks of bitmaps
    static final SoftRecycler<LongOneOrN.Block2[]> inUse1Recycler =
            SoftRecyclerWindow
                    .register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_CAPACITY1,
                            SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_MAXIMUM_CAPACITY1,
                            () -> new LongOneOrN.Block2[BLOCK1_SIZE],
                            null));

    // the highest level block of blocks of blocks of inUse bitmaps
    static final SoftRecycler<LongOneOrN.Block1[]> inUse0Recycler =
            SoftRecyclerWindow
                    .register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_CAPACITY0,
                            SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_MAXIMUM_CAPACITY0,
                            () -> new LongOneOrN.Block1[BLOCK0_SIZE],
                            null));

    transient LongOneOrN.Block0 prevInUse;

    /*
     * Normally the SparseArrayColumnSource can be changed, but if we are looking a static select, for example, we know
     * that the values are never going to actually change.
     */
    boolean immutable = false;

    /**
     * The blocks to clear on the next terminal notification
     */
    RowSet blocksToClear;
    RowSet blocks2ToClear;
    RowSet blocks1ToClear;
    /**
     * If the overall result is empty, we can clear block0 in addition to the intermediate blocks.
     */
    Boolean emptyResult;

    OneOrNSparseArrayColumnSource(Class<T> type, Class<?> componentType) {
        super(type, componentType);
    }

    OneOrNSparseArrayColumnSource(Class<T> type) {
        super(type);
    }

    /**
     * At the end of this cycle, clear the provided rowsets of blocks by releasing the blocks back to the recyclers.
     *
     * @param blocksToClear the lowest level blocks to clear
     * @param removeBlocks2 Block2 structures to clear from the Block1 structures
     * @param removeBlocks1 Block1 structures to clear from the Block0 structure
     * @param empty if the resulting table is empty
     */
    @Override
    public void clearBlocks(final RowSet blocksToClear,
            final RowSet removeBlocks2,
            final RowSet removeBlocks1,
            final boolean empty) {
        if (this.blocksToClear != null) {
            throw new IllegalStateException("Cannot call blocksToClear multiple times on the same cycle!");
        }
        Assert.eqNull(blocks2ToClear, "blocks2ToClear");
        Assert.eqNull(blocks1ToClear, "blocks1ToClear");
        Assert.eqNull(emptyResult, "emptyResult");

        this.blocksToClear = blocksToClear.copy();
        this.blocks2ToClear = removeBlocks2.copy();
        this.blocks1ToClear = removeBlocks1.copy();
        this.emptyResult = empty;
    }

    // region fillChunk
    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillByKeys(dest, rowSequence);
        } else {
            fillByRanges(dest, rowSequence);
        }
    }
    // endregion fillChunk

    @Override
    public void setNull(RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            nullByKeys(rowSequence);
        } else {
            nullByRanges(rowSequence);
        }
    }

    @Override
    public void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        fillByUnRowSequence(dest, keys);
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        fillPrevByUnRowSequence(dest, keys);
    }

    abstract void fillByRanges(@NotNull WritableChunk<? super Values> dest, @NotNull RowSequence rowSequence);

    abstract void fillByKeys(@NotNull WritableChunk<? super Values> dest, @NotNull RowSequence rowSequence);

    abstract void fillByUnRowSequence(@NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keyIndices);

    abstract void fillPrevByUnRowSequence(@NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keyIndices);

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

    abstract void nullByRanges(@NotNull RowSequence rowSequence);

    abstract void nullByKeys(@NotNull RowSequence rowSequence);

    @Override
    public boolean isImmutable() {
        return immutable;
    }

    @Override
    public void setImmutable() {
        immutable = true;
    }

    protected static class FillByContext<UArray> {
        long maxKeyInCurrentBlock = -1;
        UArray block;
        int offset;
    }
}
