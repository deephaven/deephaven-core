/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.immutable.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ShiftData;
import io.deephaven.util.SoftRecycler;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

/**
 * A ColumnSource backed by in-memory arrays of data.
 *
 * The column source is dense with keys from 0 to capacity, there can be no holes.  Arrays are divided into blocks
 * so that the column source can be incrementally expanded without copying data from one array to another.
 */
@AbstractColumnSource.IsSerializable(value = true)
public abstract class ArrayBackedColumnSource<T>
        extends AbstractDeferredGroupingColumnSource<T>
        implements FillUnordered, ShiftData.ShiftCallback, WritableSource<T>, Serializable {
    private static final long serialVersionUID = -7823391894248382929L;

    static final int DEFAULT_RECYCLER_CAPACITY = 1024;
    /**
     * Initial number of blocks to allocate.
     * <p>
     * <em>Must</em> be consistent with {@link #INITIAL_MAX_INDEX}.
     * <p>
     * If this is changed, constructors will almost certainly need to be updated.
     */
    static final int INITIAL_NUMBER_OF_BLOCKS = 0;
    /**
     * Initial highest slot available without a call to {@link #ensureCapacity(long)}.
     * <p>
     * <em>Must</em> be consistent with {@link #INITIAL_NUMBER_OF_BLOCKS}.
     * <p>
     * If this is changed, constructors will almost certainly need to be updated.
     */
    static final long INITIAL_MAX_INDEX = -1L;

    // Usage:
    //   final int block = (int) (key >> LOG_BLOCK_SIZE);
    //   final int indexWithinBlock = (int) (key & indexMask);
    //   data = blocks[block][indexWithinBlock];
    static final int LOG_BLOCK_SIZE = 11;  // Must be >= LOG_MASK_SIZE
    static final long INDEX_MASK = (1 << LOG_BLOCK_SIZE) - 1;
    public static final int BLOCK_SIZE = 1 << LOG_BLOCK_SIZE;

    // The inUse calculations are confusing because there are actually three levels of indexing (where the third level
    // is really an index into a bitmask). In pseudocode:
    //   bool inUse = prevInUse[block][indexWithinInUse][inUseBitIndex]
    // Or, in actual code
    //   bool inUse = (prevInUse[block][indexWithinInUse] & maskWithinInUse) != 0
    //
    // Where
    //   block = as above
    //   final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE
    //   final long maskWithinInUse = 1L << (indexWithinBlock & inUseMask);
    static final int LOG_INUSE_BITSET_SIZE = 6;
    private static final int LOG_INUSE_BLOCK_SIZE = LOG_BLOCK_SIZE - LOG_INUSE_BITSET_SIZE;
    private static final int IN_USE_BLOCK_SIZE = 1 << LOG_INUSE_BLOCK_SIZE;
    static final int IN_USE_MASK = (1 << LOG_INUSE_BITSET_SIZE) - 1;

    /**
     * Minimum average run length in an {@link OrderedKeys} that should trigger
     * {@link Chunk}-filling by key ranges instead of individual keys.
     */
    static final long USE_RANGES_AVERAGE_RUN_LENGTH = 5;

    static final SoftRecycler<long[]> inUseRecycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new long[IN_USE_BLOCK_SIZE],
            block -> Arrays.fill(block, 0));

    /**
     * The highest slot that can be used without a call to {@link #ensureCapacity(long)}.
     */
    long maxIndex;

    /**
     * A bitset with the same two-level structure as the array-backed data, except that the inner array is interpreted
     * as a bitset (and is thus not very big... its length is blockSize / 64, and because blockSize is currently 256,
     * the size of the inner array is 4.
     */
    transient long[][] prevInUse;

    ArrayBackedColumnSource(Class<T> type, Class componentType) {
        super(type, componentType);
    }

    ArrayBackedColumnSource(Class<T> type) {
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
        throw new UnsupportedOperationException("" + this.getClass());
    }

    @Override
    public void set(long key, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long key, short value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Produces an ArrayBackedColumnSource with the given data.
     *
     * @param data a collection containing the data to insert into the ColumnSource.
     * @param type the type of the resulting column source
     *
     * @return an in-memory column source with the requested data
     */
    public static <T> ArrayBackedColumnSource<T> getMemoryColumnSource(Collection<T> data, Class<T> type) {
        final ArrayBackedColumnSource<T> result = getMemoryColumnSource(data.size(), type);
        long i = 0;
        for (T o : data) {
            result.set(i++, o);
        }
        return result;
    }

    /**
     * Produces an ArrayBackedColumnSource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     * @param type the type of the resulting column source
     *
     * @return an in-memory column source with the requested data
     */
    public static <T> ArrayBackedColumnSource<T> getMemoryColumnSource(T[] data, Class<T> type) {
        final ArrayBackedColumnSource<T> result = getMemoryColumnSource(data.length, type);
        long i = 0;
        for (T o : data) {
            result.set(i++, o);
        }
        return result;
    }

    /**
     * Produces an ByteArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     *
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Byte> getMemoryColumnSource(byte[] data) {
        final ArrayBackedColumnSource<Byte> result = new ByteArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (byte o : data) {
            result.set(i++, o);
        }
        return result;
    }

    /**
     * Produces an CharacterArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     *
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Character> getMemoryColumnSource(char[] data) {
        final ArrayBackedColumnSource<Character> result = new CharacterArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (char o : data) {
            result.set(i++, o);
        }
        return result;
    }

    /**
     * Produces an DoubleArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     *
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Double> getMemoryColumnSource(double[] data) {
        final ArrayBackedColumnSource<Double> result = new DoubleArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (double o : data) {
            result.set(i++, o);
        }
        return result;
    }

    /**
     * Produces an FloatArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     *
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Float> getMemoryColumnSource(float[] data) {
        final ArrayBackedColumnSource<Float> result = new FloatArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (float o : data) {
            result.set(i++, o);
        }
        return result;
    }

    /**
     * Produces an IntegerArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     *
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Integer> getMemoryColumnSource(int[] data) {
        final ArrayBackedColumnSource<Integer> result = new IntegerArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (int o : data) {
            result.set(i++, o);
        }
        return result;
    }

    /**
     * Produces an LongArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     *
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Long> getMemoryColumnSource(long[] data) {
        final ArrayBackedColumnSource<Long> result = new LongArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (long o : data) {
            result.set(i++, o);
        }
        return result;
    }

    /**
     * Produces an DateTimeArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource, represented as long nanoseconds since
     *             the epoch
     *
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<DBDateTime> getDateTimeMemoryColumnSource(long[] data) {
        final ArrayBackedColumnSource<DBDateTime> result = new DateTimeArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (long o : data) {
            result.set(i++, o);
        }
        return result;
    }

    /**
     * Produces an ShortArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     *
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Short> getMemoryColumnSource(short[] data) {
        final ArrayBackedColumnSource<Short> result = new ShortArraySource();
        result.ensureCapacity(data.length);
        long i = 0;
        for (short o : data) {
            result.set(i++, o);
        }
        return result;
    }

    /**
     * Produces an empty ArrayBackedColumnSource with the given type and capacity.
     *
     * @param size the capacity of the returned column source
     * @param type the type of the resultant column source
     * @param <T> the type parameter for the ColumnSource's type
     *
     * @return an in-memory column source of the requested type
     */
    public static <T> ArrayBackedColumnSource<T> getMemoryColumnSource(long size, Class<T> type) {
        return getMemoryColumnSource(size, type, null);
    }

    public static <T> ArrayBackedColumnSource<T> getMemoryColumnSource(Class<T> type, @Nullable Class componentType) {
        return getMemoryColumnSource(0, type, componentType);
    }

    /**
     * Produces an empty ArrayBackedColumnSource with the given type and capacity.
     *
     * @param size the capacity of the returned column source
     * @param type the type of the resultant column source
     * @param componentType the component type for column sources of arrays or DbArrays
     * @param <T> the type parameter for the ColumnSource's type
     *
     * @return an in-memory column source of the requested type
     */
    public static <T> ArrayBackedColumnSource<T> getMemoryColumnSource(long size, Class<T> type, @Nullable Class componentType) {
        final ArrayBackedColumnSource result;
        if (type == byte.class || type == Byte.class) {
            result = new ByteArraySource();
        } else if (type == char.class || type == Character.class) {
            result = new CharacterArraySource();
        } else if (type == double.class || type == Double.class) {
            result = new DoubleArraySource();
        } else if (type == float.class || type == Float.class) {
            result = new FloatArraySource();
        } else if (type == int.class || type == Integer.class) {
            result = new IntegerArraySource();
        } else if (type == long.class || type == Long.class) {
            result = new LongArraySource();
        } else if (type == short.class || type == Short.class) {
            result = new ShortArraySource();
        } else if (type == boolean.class || type == Boolean.class) {
            result = new BooleanArraySource();
        } else if (type == DBDateTime.class) {
            result = new DateTimeArraySource();
        } else {
            if (componentType != null) {
                result = new ObjectArraySource<>(type, componentType);
            } else {
                result = new ObjectArraySource<>(type);
            }
        }
        if (size > 0) {
            result.ensureCapacity(size);
        }
        //noinspection unchecked
        return result;
    }

    public abstract void ensureCapacity(long size, boolean nullFill);

    @Override
    public void shift(long start, long end, long offset) {
        if (offset > 0) {
            for (long i = end; i >= start; i--) {
                set((i + offset), get(i));
            }
        } else {
            for (long i = start; i <= end; i++) {
                set((i + offset), get(i));
            }
        }

    }

    /**
     * Creates an in-memory ColumnSource from the supplied dataArray, using instanceof checks to determine the
     * appropriate type of column source to produce.
     *
     * @param dataArray the data to insert into the new column source
     *
     * @return a ColumnSource with the supplied data.
     */
    public static WritableSource getMemoryColumnSource(Object dataArray) {
        if (dataArray instanceof boolean[]) {
            return getMemoryColumnSource(ArrayUtils.getBoxedArray((boolean[]) dataArray), Boolean.class);
        } else if (dataArray instanceof byte[]) {
            return getMemoryColumnSource((byte[]) dataArray);
        } else if (dataArray instanceof char[]) {
            return getMemoryColumnSource((char[]) dataArray);
        } else if (dataArray instanceof double[]) {
            return getMemoryColumnSource((double[]) dataArray);
        } else if (dataArray instanceof float[]) {
            return getMemoryColumnSource((float[]) dataArray);
        } else if (dataArray instanceof int[]) {
            return getMemoryColumnSource((int[]) dataArray);
        } else if (dataArray instanceof long[]) {
            return getMemoryColumnSource((long[]) dataArray);
        } else if (dataArray instanceof short[]) {
            return getMemoryColumnSource((short[]) dataArray);
        } else if (dataArray instanceof Boolean[]) {
            return getMemoryColumnSource((Boolean[]) dataArray, Boolean.class);
        } else if (dataArray instanceof Byte[]) {
            return getMemoryColumnSource(ArrayUtils.getUnboxedArray((Byte[]) dataArray));
        } else if (dataArray instanceof Character[]) {
            return getMemoryColumnSource(ArrayUtils.getUnboxedArray((Character[]) dataArray));
        } else if (dataArray instanceof Double[]) {
            return getMemoryColumnSource(ArrayUtils.getUnboxedArray((Double[]) dataArray));
        } else if (dataArray instanceof Float[]) {
            return getMemoryColumnSource(ArrayUtils.getUnboxedArray((Float[]) dataArray));
        } else if (dataArray instanceof Integer[]) {
            return getMemoryColumnSource(ArrayUtils.getUnboxedArray((Integer[]) dataArray));
        } else if (dataArray instanceof Long[]) {
            return getMemoryColumnSource(ArrayUtils.getUnboxedArray((Long[]) dataArray));
        } else if (dataArray instanceof Short[]) {
            return getMemoryColumnSource(ArrayUtils.getUnboxedArray((Short[]) dataArray));
        } else {
            //noinspection unchecked
            return getMemoryColumnSource((Object[]) dataArray, (Class<Object>) dataArray.getClass().getComponentType());
        }
    }

    /**
     * Wrap the input array in an immutable {@link ColumnSource}. This method will unbox any boxed values,
     * and directly use the result array.
     *
     * @param dataArray The array to turn into a ColumnSource
     * @return An Immutable ColumnSource that directly wraps the input array.
     */
    public static ColumnSource getImmutableMemoryColumnSource(@NotNull  Object dataArray) {
        final Class arrayType = dataArray.getClass().getComponentType();
        if(arrayType == null) {
            throw new IllegalArgumentException("Input value was not an array, was " + dataArray.getClass().getName());
        }

        return getImmutableMemoryColumnSource(dataArray, arrayType);
    }

    /**
     * Wrap the input array in an immutable {@link ColumnSource}. This method will unbox any boxed values,
     * and directly use the result array.  This version allows the user to specify the column type.  It will
     * automatically map column type Boolean/boolean with input array types byte[] to {@link ImmutableBooleanArraySource}
     * and columnType DBDateTime / array type long[] to {@link ImmutableDateTimeArraySource}
     *
     * @param dataArray The array to turn into a ColumnSource
     *
     * @return An Immutable ColumnSource that directly wraps the input array.
     */
    public static ColumnSource getImmutableMemoryColumnSource(Object dataArray, Class<?> type) {
        if (type == boolean.class) {
            return (dataArray instanceof byte[]) ? new ImmutableBooleanArraySource((byte[]) dataArray)
                                                 : new ImmutableBooleanArraySource((boolean[]) dataArray);
        } else if (type == byte.class) {
            return new ImmutableByteArraySource((byte[]) dataArray);
        } else if (type == char.class) {
            return new ImmutableCharArraySource((char[]) dataArray);
        } else if (type == double.class) {
            return new ImmutableDoubleArraySource((double[]) dataArray);
        } else if (type == float.class) {
            return new ImmutableFloatArraySource((float[]) dataArray);
        } else if (type == int.class) {
            return new ImmutableIntArraySource((int[]) dataArray);
        } else if (type == long.class) {
            return new ImmutableLongArraySource((long[]) dataArray);
        } else if (type == short.class) {
            return new ImmutableShortArraySource((short[]) dataArray);
        } else if (type == Boolean.class) {
            return (dataArray instanceof byte[]) ? new ImmutableBooleanArraySource((byte[]) dataArray)
                                                 : new ImmutableBooleanArraySource((Boolean[]) dataArray);
        } else if (type == Byte.class) {
            return new ImmutableByteArraySource(ArrayUtils.getUnboxedArray((Byte[]) dataArray));
        } else if (type == Character.class) {
            return new ImmutableCharArraySource(ArrayUtils.getUnboxedArray((Character[]) dataArray));
        } else if (type == Double.class) {
            return new ImmutableDoubleArraySource(ArrayUtils.getUnboxedArray((Double[]) dataArray));
        } else if (type == Float.class) {
            return new ImmutableFloatArraySource(ArrayUtils.getUnboxedArray((Float[]) dataArray));
        } else if (type == Integer.class) {
            return new ImmutableIntArraySource(ArrayUtils.getUnboxedArray((Integer[]) dataArray));
        } else if (type == Long.class) {
            return new ImmutableLongArraySource(ArrayUtils.getUnboxedArray((Long[]) dataArray));
        } else if (type == Short.class) {
            return new ImmutableShortArraySource(ArrayUtils.getUnboxedArray((Short[]) dataArray));
        } else if (type == DBDateTime.class && dataArray instanceof long[]) {
            return new ImmutableDateTimeArraySource((long[]) dataArray);
        } else {
            //noinspection unchecked
            return new ImmutableObjectArraySource((Object[]) dataArray, dataArray.getClass().getComponentType());
        }
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    static int getBlockNo(long from) {
        return (int) (from >> LOG_BLOCK_SIZE);
    }

    abstract Object getBlock(int blockIndex);

    abstract Object getPrevBlock(int blockIndex);

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        if (orderedKeys.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillSparseChunk(destination, orderedKeys);
            return;
        }
        MutableInt destOffset = new MutableInt(0);
        orderedKeys.forAllLongRanges((final long from, final long to) -> {
            final int fromBlock = getBlockNo(from);
            final int toBlock = getBlockNo(to);
            final int fromOffsetInBlock = (int) (from & INDEX_MASK);
            if (fromBlock == toBlock) {
                final int sz = LongSizedDataStructure.intSize("int cast", to - from + 1);
                destination.copyFromArray(getBlock(fromBlock), fromOffsetInBlock, destOffset.intValue(), sz);
                destOffset.add(sz);
            } else {
                final int sz = BLOCK_SIZE - fromOffsetInBlock;
                destination.copyFromArray(getBlock(fromBlock), fromOffsetInBlock, destOffset.intValue(), sz);
                destOffset.add(sz);
                for (int blockNo = fromBlock + 1; blockNo < toBlock; ++blockNo) {
                    destination.copyFromArray(getBlock(blockNo), 0, destOffset.intValue(), BLOCK_SIZE);
                    destOffset.add(BLOCK_SIZE);
                }
                int restSz = (int) (to & INDEX_MASK) + 1;
                destination.copyFromArray(getBlock(toBlock), 0, destOffset.intValue(), restSz);
                destOffset.add(restSz);
            }
        });
        destination.setSize(destOffset.intValue());
    }

    @Override
    public void fillChunkUnordered(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final LongChunk<? extends KeyIndices> keyIndices) {
        fillSparseChunkUnordered(destination, keyIndices);
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final LongChunk<? extends KeyIndices> keyIndices) {
        fillSparsePrevChunkUnordered(destination, keyIndices);
    }

    /**
     * Resets the given chunk to provide a write-through reference to our backing array.
     *
     * Note: This is unsafe to use if previous tracking has been enabled!
     *
     * @param chunk the writable chunk to reset to our backing array.
     * @param position position that we require
     * @return the first position addressable by the chunk
     */
    public abstract long resetWritableChunkToBackingStore(@NotNull final ResettableWritableChunk chunk, long position);

    protected abstract void fillSparseChunk(@NotNull WritableChunk<? super Values> destination, @NotNull OrderedKeys indices);

    protected abstract void fillSparsePrevChunk(@NotNull WritableChunk<? super Values> destination, @NotNull OrderedKeys indices);

    protected abstract void fillSparseChunkUnordered(@NotNull WritableChunk<? super Values> destination, @NotNull LongChunk<? extends KeyIndices> indices);

    protected abstract void fillSparsePrevChunkUnordered(@NotNull WritableChunk<? super Values> destination, @NotNull LongChunk<? extends KeyIndices> indices);

    @Override
    public Chunk<Values> getChunk(@NotNull final GetContext context, @NotNull final OrderedKeys orderedKeys) {
        final LongChunk<OrderedKeyRanges> ranges = orderedKeys.asKeyRangesChunk();
        if (ranges.size() == 2) {
            final long first = ranges.get(0);
            final long last = ranges.get(1);

            if (getBlockNo(first) == getBlockNo(last)) {
                // Optimization if the caller requests a single range which happens to fall within a single block
                final int blockPos = getBlockNo(first);
                return DefaultGetContext.resetChunkFromArray(context,
                        getBlock(blockPos), (int) (first & INDEX_MASK), (int) (last - first + 1));
            }
        }

        return getChunkByFilling(context, orderedKeys);
    }
}
