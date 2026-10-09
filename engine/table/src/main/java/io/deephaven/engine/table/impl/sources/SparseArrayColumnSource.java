//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.rowset.RowSetShiftCallback;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.impl.sources.sparse.LongOneOrN;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.SoftRecycler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.*;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;

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
 * </p>
 */
public abstract class SparseArrayColumnSource<T>
        extends AbstractColumnSource<T>
        implements FillUnordered<Values>, WritableColumnSource<T>, InMemoryColumnSource, PossiblyImmutableColumnSource,
        WritableSourceWithPrepareForParallelPopulation, RowSetShiftCallback {


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
        // we must completely use the 63-bit address space of row keys (negative numbers are defined to be null)
        Assert.eq(LOG_BLOCK_SIZE + LOG_BLOCK0_SIZE + LOG_BLOCK1_SIZE + LOG_BLOCK2_SIZE,
                "LOG_BLOCK_SIZE + LOG_BLOCK0_SIZE + LOG_BLOCK1_SIZE + LOG_BLOCK2_SIZE", 63);
        Assert.geq(LOG_BLOCK_SIZE, "LOG_BLOCK_SIZE", LOG_INUSE_BITSET_SIZE);
    }

    // the lowest level inUse bitmap recycle
    static final SoftRecycler<long[]> inUseRecycler =
            new SoftRecycler<>(SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_CAPACITY,
                    () -> new long[IN_USE_BLOCK_SIZE],
                    block -> Arrays.fill(block, 0));

    // the recycler for blocks of bitmaps
    static final SoftRecycler<long[][]> inUse2Recycler =
            new SoftRecycler<>(SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_CAPACITY2,
                    () -> new long[BLOCK2_SIZE][],
                    null);

    // the recycler for blocks of blocks of bitmaps
    static final SoftRecycler<LongOneOrN.Block2[]> inUse1Recycler =
            new SoftRecycler<>(SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_CAPACITY1,
                    () -> new LongOneOrN.Block2[BLOCK1_SIZE],
                    null);

    // the highest level block of blocks of blocks of inUse bitmaps
    static final SoftRecycler<LongOneOrN.Block1[]> inUse0Recycler =
            new SoftRecycler<>(SparseArrayColumnSourceConfiguration.IN_USE_RECYCLER_CAPACITY0,
                    () -> new LongOneOrN.Block1[BLOCK0_SIZE],
                    null);

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

    SparseArrayColumnSource(Class<T> type, Class<?> componentType) {
        super(type, componentType);
    }

    SparseArrayColumnSource(Class<T> type) {
        super(type);
    }

    @Override
    public void set(long key, byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long key, char value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long key, double value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long key, float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long key, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long key, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long key, short value) {
        throw new UnsupportedOperationException();
    }

    public void remove(RowSet toRemove) {
        setNull(toRemove);
    }

    /**
     * At the end of this cycle, clear the provided rowsets of blocks by releasing the blocks back to the recyclers.
     *
     * @param blocksToClear the lowest level blocks to clear
     * @param removeBlocks2 Block2 structures to clear from the Block1 structures
     * @param removeBlocks1 Block1 structures to clear from the Block0 structure
     * @param empty if the resulting table is empty
     */
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

    /**
     * At the end of this cycle, release each block, block2 and block1 structure of {@code sources} that holds a row key
     * of {@code candidateRowKeys} and no row key of {@code liveRowSet}.
     *
     * <p>
     * The values each source holds at row keys outside {@code liveRowSet} must be unused after this cycle. The cost is
     * proportional to the number of blocks that {@code candidateRowKeys} spans, so it should hold the row keys that
     * were live at the start of this cycle and are not live now (the removed rows and the rows that shifts vacated),
     * and as few others as practical. Each source may be passed to this method, or to
     * {@link #clearBlocks(RowSet, RowSet, RowSet, boolean)}, at most once per cycle.
     * </p>
     *
     * @param candidateRowKeys the row keys whose blocks may have no live rows
     * @param liveRowSet the row keys whose values the sources hold after this cycle
     * @param sources the sources to release blocks from
     */
    public static void clearBlocksWithoutLiveRows(
            @NotNull final RowSet candidateRowKeys,
            @NotNull final RowSet liveRowSet,
            @NotNull final SparseArrayColumnSource<?>... sources) {
        if (sources.length == 0 || candidateRowKeys.isEmpty()) {
            return;
        }
        try (final RowSet removeBlocks = getBlocksWithoutLiveRows(candidateRowKeys, liveRowSet, LOG_BLOCK_SIZE);
                final RowSet removeBlocks2 = removeBlocks.isNonempty()
                        ? getBlocksWithoutLiveRows(candidateRowKeys, liveRowSet, BLOCK1_SHIFT)
                        : RowSetFactory.empty();
                final RowSet removeBlocks1 = removeBlocks2.isNonempty()
                        ? getBlocksWithoutLiveRows(candidateRowKeys, liveRowSet, BLOCK0_SHIFT)
                        : RowSetFactory.empty()) {
            if (removeBlocks.isEmpty()) {
                return;
            }
            for (final SparseArrayColumnSource<?> source : sources) {
                source.clearBlocks(removeBlocks, removeBlocks2, removeBlocks1, liveRowSet.isEmpty());
            }
        }
    }

    /**
     * At the end of this cycle, release each block, block2 and block1 structure of {@code source} that holds a row key
     * vacated this cycle and no row key of {@code outerRowSet}. The vacated row keys are those removed and those within
     * the range of a shift, as {@link #clearBlocksWithoutLiveRows(RowSet, RowSet, SparseArrayColumnSource[])} requires;
     * the shifted row keys that are still live keep their blocks.
     *
     * @param removed the row keys removed from {@code outerRowSet} this cycle, in the pre-shift key space
     * @param shifted the shifts applied to {@code outerRowSet} this cycle
     * @param outerRowSet the row keys whose values {@code source} holds, whose previous value is the row set at the
     *        start of this cycle
     * @param source the source to release blocks from
     */
    public static void clearVacatedBlocks(
            @NotNull final RowSet removed,
            @NotNull final RowSetShiftData shifted,
            @NotNull final TrackingRowSet outerRowSet,
            @NotNull final SparseArrayColumnSource<?> source) {
        if (shifted.empty()) {
            clearBlocksWithoutLiveRows(removed, outerRowSet, source);
            return;
        }
        final RowSet prevRowSet = outerRowSet.prev();
        try (final WritableRowSet candidates = removed.copy()) {
            final int shiftCount = shifted.size();
            for (int ii = 0; ii < shiftCount; ++ii) {
                try (final RowSet shiftedRows =
                        prevRowSet.subSetByKeyRange(shifted.getBeginRange(ii), shifted.getEndRange(ii))) {
                    candidates.insert(shiftedRows);
                }
            }
            clearBlocksWithoutLiveRows(candidates, outerRowSet, source);
        }
    }

    /**
     * @return the indices ({@code rowKey >> logBlockSize}) of the blocks of {@code 1 << logBlockSize} row keys that
     *         hold a row key of {@code candidateRowKeys} and no row key of {@code liveRowSet}
     */
    @NotNull
    private static RowSet getBlocksWithoutLiveRows(
            final RowSet candidateRowKeys,
            final RowSet liveRowSet,
            final int logBlockSize) {
        final long blockSize = 1L << logBlockSize;
        final RowSet.SearchIterator liveIterator = liveRowSet.searchIterator();
        final RowSet.SearchIterator candidateIterator = candidateRowKeys.searchIterator();

        final RowSetBuilderSequential removeBlockBuilder = RowSetFactory.builderSequential();
        long startOfNextBlock = 0;
        while (candidateIterator.advance(startOfNextBlock)) {
            final long candidateKey = candidateIterator.currentValue();
            final long endOfCandidateBlock = candidateKey | (blockSize - 1);

            final long startOfCandidateBlock = candidateKey & ~(blockSize - 1);
            if (!liveIterator.advance(startOfCandidateBlock) || liveIterator.currentValue() > endOfCandidateBlock) {
                removeBlockBuilder.appendKey(candidateKey >> logBlockSize);
            }
            if (endOfCandidateBlock == Long.MAX_VALUE) {
                break;
            }
            startOfNextBlock = endOfCandidateBlock + 1;
        }
        return removeBlockBuilder.build();
    }

    public static <T> WritableColumnSource<T> getSparseMemoryColumnSource(Collection<T> data, Class<T> type) {
        final WritableColumnSource<T> result = getSparseMemoryColumnSource(data.size(), type);
        long i = 0;
        for (T o : data) {
            result.set(i++, o);
        }
        return result;
    }

    private static <T> WritableColumnSource<T> getSparseMemoryColumnSource(T[] data, Class<T> type) {
        final WritableColumnSource<T> result = getSparseMemoryColumnSource(data.length, type);
        long i = 0;
        for (T o : data) {
            result.set(i++, o);
        }
        return result;
    }

    public static SparseArrayColumnSource<Byte> getSparseMemoryColumnSource(byte[] data) {
        final SparseArrayColumnSource<Byte> result = new ByteSparseArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (byte o : data) {
            result.set(i++, o);
        }
        return result;
    }

    public static SparseArrayColumnSource<Character> getSparseMemoryColumnSource(char[] data) {
        final SparseArrayColumnSource<Character> result = new CharacterSparseArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (char o : data) {
            result.set(i++, o);
        }
        return result;
    }

    public static SparseArrayColumnSource<Double> getSparseMemoryColumnSource(double[] data) {
        final SparseArrayColumnSource<Double> result = new DoubleSparseArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (double o : data) {
            result.set(i++, o);
        }
        return result;
    }

    public static SparseArrayColumnSource<Float> getSparseMemoryColumnSource(float[] data) {
        final SparseArrayColumnSource<Float> result = new FloatSparseArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (float o : data) {
            result.set(i++, o);
        }
        return result;
    }

    public static SparseArrayColumnSource<Integer> getSparseMemoryColumnSource(int[] data) {
        final SparseArrayColumnSource<Integer> result = new IntegerSparseArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (int o : data) {
            result.set(i++, o);
        }
        return result;
    }

    public static SparseArrayColumnSource<Long> getSparseMemoryColumnSource(long[] data) {
        final SparseArrayColumnSource<Long> result = new LongSparseArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (long o : data) {
            result.set(i++, o);
        }
        return result;
    }

    public static WritableColumnSource<Instant> getInstantMemoryColumnSource(long[] data) {
        final WritableColumnSource<Instant> result = new InstantSparseArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (long o : data) {
            result.set(i++, o);
        }
        return result;
    }

    public static SparseArrayColumnSource<Short> getSparseMemoryColumnSource(short[] data) {
        final SparseArrayColumnSource<Short> result = new ShortSparseArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (short o : data) {
            result.set(i++, o);
        }
        return result;
    }

    public static <T> WritableColumnSource<T> getSparseMemoryColumnSource(Class<T> type) {
        return getSparseMemoryColumnSource(0, type, null);
    }

    public static <T> WritableColumnSource<T> getSparseMemoryColumnSource(Class<T> type, Class<?> componentType) {
        return getSparseMemoryColumnSource(0, type, componentType);
    }

    public static <T> WritableColumnSource<T> getSparseMemoryColumnSource(long size, Class<T> type) {
        return getSparseMemoryColumnSource(size, type, null);
    }

    public static <T> WritableColumnSource<T> getSparseMemoryColumnSource(long size, Class<T> type,
            @Nullable Class<?> componentType) {
        final WritableColumnSource<?> result;
        if (type == byte.class || type == Byte.class) {
            result = new ByteSparseArraySource();
        } else if (type == char.class || type == Character.class) {
            result = new CharacterSparseArraySource();
        } else if (type == double.class || type == Double.class) {
            result = new DoubleSparseArraySource();
        } else if (type == float.class || type == Float.class) {
            result = new FloatSparseArraySource();
        } else if (type == int.class || type == Integer.class) {
            result = new IntegerSparseArraySource();
        } else if (type == long.class || type == Long.class) {
            result = new LongSparseArraySource();
        } else if (type == short.class || type == Short.class) {
            result = new ShortSparseArraySource();
        } else if (type == boolean.class || type == Boolean.class) {
            result = new BooleanSparseArraySource();
        } else if (type == Instant.class) {
            result = new InstantSparseArraySource();
        } else {
            if (componentType != null) {
                result = new ObjectSparseArraySource<>(type, componentType);
            } else {
                result = new ObjectSparseArraySource<>(type);
            }
        }
        if (size != 0) {
            result.ensureCapacity(size);
        }
        // noinspection unchecked
        return (WritableColumnSource<T>) result;
    }

    public static ColumnSource<?> getSparseMemoryColumnSource(Object dataArray) {
        if (dataArray instanceof boolean[]) {
            return getSparseMemoryColumnSource(ArrayTypeUtils.getBoxedArray((boolean[]) dataArray), Boolean.class);
        } else if (dataArray instanceof byte[]) {
            return getSparseMemoryColumnSource((byte[]) dataArray);
        } else if (dataArray instanceof char[]) {
            return getSparseMemoryColumnSource((char[]) dataArray);
        } else if (dataArray instanceof double[]) {
            return getSparseMemoryColumnSource((double[]) dataArray);
        } else if (dataArray instanceof float[]) {
            return getSparseMemoryColumnSource((float[]) dataArray);
        } else if (dataArray instanceof int[]) {
            return getSparseMemoryColumnSource((int[]) dataArray);
        } else if (dataArray instanceof long[]) {
            return getSparseMemoryColumnSource((long[]) dataArray);
        } else if (dataArray instanceof short[]) {
            return getSparseMemoryColumnSource((short[]) dataArray);
        } else if (dataArray instanceof Boolean[]) {
            return getSparseMemoryColumnSource((Boolean[]) dataArray, Boolean.class);
        } else if (dataArray instanceof Byte[]) {
            return getSparseMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Byte[]) dataArray));
        } else if (dataArray instanceof Character[]) {
            return getSparseMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Character[]) dataArray));
        } else if (dataArray instanceof Double[]) {
            return getSparseMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Double[]) dataArray));
        } else if (dataArray instanceof Float[]) {
            return getSparseMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Float[]) dataArray));
        } else if (dataArray instanceof Integer[]) {
            return getSparseMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Integer[]) dataArray));
        } else if (dataArray instanceof Long[]) {
            return getSparseMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Long[]) dataArray));
        } else if (dataArray instanceof Short[]) {
            return getSparseMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Short[]) dataArray));
        } else {
            // noinspection unchecked
            return getSparseMemoryColumnSource((Object[]) dataArray,
                    (Class<Object>) dataArray.getClass().getComponentType());
        }
    }

    /**
     * Using a preferred chunk size of BLOCK_SIZE gives us the opportunity to directly return chunks from our data
     * structure rather than copying data.
     */
    public int getPreferredChunkSize() {
        return BLOCK_SIZE;
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

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    /**
     * Return an estimate of the heap size taken by the current values within this sparse array source.
     *
     * <p>
     * Only leaf nodes and the size arrays of references are included in this estimate. Intermediate objects are
     * ignored, and an array of references is assumed to take 8 bytes per element with no overhead.
     * </p>
     *
     * @return an estimate of the size of this column source's current data
     */
    @TestUseOnly
    abstract public long estimateSize();
}
