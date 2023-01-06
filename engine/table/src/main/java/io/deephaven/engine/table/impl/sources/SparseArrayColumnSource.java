/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.time.DateTime;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.impl.sources.sparse.LongOneOrN;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.SoftRecycler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.*;

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
        extends AbstractDeferredGroupingColumnSource<T>
        implements FillUnordered<Values>, WritableColumnSource<T>, InMemoryColumnSource, PossiblyImmutableColumnSource,
        WritableSourceWithPrepareForParallelPopulation {
    public static final SparseArrayColumnSource<?>[] ZERO_LENGTH_SPARSE_ARRAY_COLUMN_SOURCE_ARRAY =
            new SparseArrayColumnSource[0];

    static final int DEFAULT_RECYCLER_CAPACITY = 1024;

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
    static final SoftRecycler<long[]> inUseRecycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new long[IN_USE_BLOCK_SIZE],
            block -> Arrays.fill(block, 0));

    // the recycler for blocks of bitmaps
    static final SoftRecycler<long[][]> inUse2Recycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new long[BLOCK2_SIZE][],
            null);

    // the recycler for blocks of blocks of bitmaps
    static final SoftRecycler<LongOneOrN.Block2[]> inUse1Recycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new LongOneOrN.Block2[BLOCK1_SIZE],
            null);

    // the highest level block of blocks of blocks of inUse bitmaps
    static final SoftRecycler<LongOneOrN.Block1[]> inUse0Recycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new LongOneOrN.Block1[BLOCK0_SIZE],
            null);

    transient LongOneOrN.Block0 prevInUse;

    /*
     * Normally the SparseArrayColumnSource can be changed, but if we are looking a static select, for example, we know
     * that the values are never going to actually change.
     */
    boolean immutable = false;

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

    public void shift(RowSet keysToShift, long shiftDelta) {
        throw new UnsupportedOperationException();
    }

    public void remove(RowSet toRemove) {
        throw new UnsupportedOperationException();
    }

    public static <T> SparseArrayColumnSource<T> getSparseMemoryColumnSource(Collection<T> data, Class<T> type) {
        final SparseArrayColumnSource<T> result = getSparseMemoryColumnSource(data.size(), type);
        long i = 0;
        for (T o : data) {
            result.set(i++, o);
        }
        return result;
    }

    private static <T> SparseArrayColumnSource<T> getSparseMemoryColumnSource(T[] data, Class<T> type) {
        final SparseArrayColumnSource<T> result = getSparseMemoryColumnSource(data.length, type);
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

    public static SparseArrayColumnSource<DateTime> getDateTimeMemoryColumnSource(long[] data) {
        final SparseArrayColumnSource<DateTime> result = new DateTimeSparseArraySource();
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

    public static <T> SparseArrayColumnSource<T> getSparseMemoryColumnSource(Class<T> type) {
        return getSparseMemoryColumnSource(0, type, null);
    }

    public static <T> SparseArrayColumnSource<T> getSparseMemoryColumnSource(Class<T> type, Class<?> componentType) {
        return getSparseMemoryColumnSource(0, type, componentType);
    }

    public static <T> SparseArrayColumnSource<T> getSparseMemoryColumnSource(long size, Class<T> type) {
        return getSparseMemoryColumnSource(size, type, null);
    }

    public static <T> SparseArrayColumnSource<T> getSparseMemoryColumnSource(long size, Class<T> type,
            @Nullable Class<?> componentType) {
        final SparseArrayColumnSource<?> result;
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
        } else if (type == DateTime.class) {
            result = new DateTimeSparseArraySource();
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
        return (SparseArrayColumnSource<T>) result;
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
}
