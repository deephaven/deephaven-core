/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ByteChunk;
import io.deephaven.db.v2.sources.chunk.CharChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.DefaultGetContext;
import io.deephaven.db.v2.sources.chunk.DoubleChunk;
import io.deephaven.db.v2.sources.chunk.FloatChunk;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.ResettableWritableChunk;
import io.deephaven.db.v2.sources.chunk.ShortChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.immutable.ImmutableBooleanArraySource;
import io.deephaven.db.v2.sources.immutable.ImmutableByteArraySource;
import io.deephaven.db.v2.sources.immutable.ImmutableCharArraySource;
import io.deephaven.db.v2.sources.immutable.ImmutableDateTimeArraySource;
import io.deephaven.db.v2.sources.immutable.ImmutableDoubleArraySource;
import io.deephaven.db.v2.sources.immutable.ImmutableFloatArraySource;
import io.deephaven.db.v2.sources.immutable.ImmutableIntArraySource;
import io.deephaven.db.v2.sources.immutable.ImmutableLongArraySource;
import io.deephaven.db.v2.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.db.v2.sources.immutable.ImmutableShortArraySource;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ShiftData;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.BooleanArray;
import io.deephaven.qst.array.ByteArray;
import io.deephaven.qst.array.CharArray;
import io.deephaven.qst.array.DoubleArray;
import io.deephaven.qst.array.FloatArray;
import io.deephaven.qst.array.GenericArray;
import io.deephaven.qst.array.IntArray;
import io.deephaven.qst.array.LongArray;
import io.deephaven.qst.array.PrimitiveArray;
import io.deephaven.qst.array.ShortArray;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType.Visitor;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
import io.deephaven.util.SoftRecycler;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * A ColumnSource backed by in-memory arrays of data.
 * <p>
 * The column source is dense with keys from 0 to capacity, there can be no holes. Arrays are
 * divided into blocks so that the column source can be incrementally expanded without copying data
 * from one array to another.
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
    // final int block = (int) (key >> LOG_BLOCK_SIZE);
    // final int indexWithinBlock = (int) (key & indexMask);
    // data = blocks[block][indexWithinBlock];
    static final int LOG_BLOCK_SIZE = 11; // Must be >= LOG_MASK_SIZE
    static final long INDEX_MASK = (1 << LOG_BLOCK_SIZE) - 1;
    public static final int BLOCK_SIZE = 1 << LOG_BLOCK_SIZE;

    // The inUse calculations are confusing because there are actually three levels of indexing
    // (where the third level
    // is really an index into a bitmask). In pseudocode:
    // bool inUse = prevInUse[block][indexWithinInUse][inUseBitIndex]
    // Or, in actual code
    // bool inUse = (prevInUse[block][indexWithinInUse] & maskWithinInUse) != 0
    //
    // Where
    // block = as above
    // final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE
    // final long maskWithinInUse = 1L << (indexWithinBlock & inUseMask);
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

    public static ArrayBackedColumnSource<?> from(Array<?> array) {
        return array.walk(new ArrayAdapter<>()).getOut();
    }

    public static <T> ArrayBackedColumnSource<T> from(PrimitiveArray<T> array) {
        ArrayAdapter<T> adapter = new ArrayAdapter<>();
        array.walk((PrimitiveArray.Visitor) adapter);
        // noinspection unchecked
        return (ArrayBackedColumnSource<T>) adapter.getOut();
    }


    /**
     * The highest slot that can be used without a call to {@link #ensureCapacity(long)}.
     */
    long maxIndex;

    /**
     * A bitset with the same two-level structure as the array-backed data, except that the inner
     * array is interpreted as a bitset (and is thus not very big... its length is blockSize / 64,
     * and because blockSize is currently 256, the size of the inner array is 4.
     */
    transient long[][] prevInUse;

    ArrayBackedColumnSource(@NotNull final Class<T> type, @Nullable final Class<?> componentType) {
        super(type, componentType);
    }

    ArrayBackedColumnSource(@NotNull final Class<T> type) {
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
     * @param dataType the data type of the resulting column source
     * @param componentType the component type of the resulting column source
     * @return an in-memory column source with the requested data
     */
    public static <T> ArrayBackedColumnSource<T> getMemoryColumnSource(
        @NotNull final Collection<T> data,
        @NotNull final Class<T> dataType,
        @Nullable final Class<?> componentType) {
        final ArrayBackedColumnSource<T> result =
            getMemoryColumnSource(data.size(), dataType, componentType);
        long i = 0;
        for (T o : data) {
            result.set(i++, o);
        }
        return result;
    }

    /**
     * Produces an ArrayBackedColumnSource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource
     * @param dataType the data type of the resulting column source
     * @param componentType the component type of the resulting column source
     * @return an in-memory column source with the requested data
     */
    public static <T> ArrayBackedColumnSource<T> getMemoryColumnSource(@NotNull final T[] data,
        @NotNull final Class<T> dataType,
        @Nullable final Class<?> componentType) {
        final ArrayBackedColumnSource<T> result =
            getMemoryColumnSource(data.length, dataType, componentType);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
            final OrderedKeys range = OrderedKeys.forRange(0, data.length - 1)) {
            result.fillFromChunk(context, ObjectChunk.chunkWrap(data), range);
        }
        return result;
    }

    /**
     * Produces an ByteArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Byte> getMemoryColumnSource(@NotNull final byte[] data) {
        final ArrayBackedColumnSource<Byte> result = new ByteArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
            final OrderedKeys range = OrderedKeys.forRange(0, data.length - 1)) {
            result.fillFromChunk(context, ByteChunk.chunkWrap(data), range);
        }
        return result;
    }

    /**
     * Produces an BooleanArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Boolean> getBooleanMemoryColumnSource(
        @NotNull final byte[] data) {
        final ArrayBackedColumnSource<Boolean> result = new BooleanArraySource();
        final WritableSource<Byte> dest = (WritableSource<Byte>) result.reinterpret(byte.class);
        result.ensureCapacity(data.length);
        try (final FillFromContext context = dest.makeFillFromContext(data.length);
            final OrderedKeys range = OrderedKeys.forRange(0, data.length - 1)) {
            dest.fillFromChunk(context, ByteChunk.chunkWrap(data), range);
        }
        return result;
    }

    /**
     * Produces an CharacterArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Character> getMemoryColumnSource(
        @NotNull final char[] data) {
        final ArrayBackedColumnSource<Character> result = new CharacterArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
            final OrderedKeys range = OrderedKeys.forRange(0, data.length - 1)) {
            result.fillFromChunk(context, CharChunk.chunkWrap(data), range);
        }
        return result;
    }

    /**
     * Produces an DoubleArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Double> getMemoryColumnSource(
        @NotNull final double[] data) {
        final ArrayBackedColumnSource<Double> result = new DoubleArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
            final OrderedKeys range = OrderedKeys.forRange(0, data.length - 1)) {
            result.fillFromChunk(context, DoubleChunk.chunkWrap(data), range);
        }
        return result;
    }

    /**
     * Produces an FloatArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Float> getMemoryColumnSource(
        @NotNull final float[] data) {
        final ArrayBackedColumnSource<Float> result = new FloatArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
            final OrderedKeys range = OrderedKeys.forRange(0, data.length - 1)) {
            result.fillFromChunk(context, FloatChunk.chunkWrap(data), range);
        }
        return result;
    }

    /**
     * Produces an IntegerArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Integer> getMemoryColumnSource(
        @NotNull final int[] data) {
        final ArrayBackedColumnSource<Integer> result = new IntegerArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
            final OrderedKeys range = OrderedKeys.forRange(0, data.length - 1)) {
            result.fillFromChunk(context, IntChunk.chunkWrap(data), range);
        }
        return result;
    }

    /**
     * Produces an LongArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Long> getMemoryColumnSource(@NotNull final long[] data) {
        final ArrayBackedColumnSource<Long> result = new LongArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
            final OrderedKeys range = OrderedKeys.forRange(0, data.length - 1)) {
            result.fillFromChunk(context, LongChunk.chunkWrap(data), range);
        }
        return result;
    }

    /**
     * Produces an DateTimeArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource, represented as long
     *        nanoseconds since the epoch
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<DBDateTime> getDateTimeMemoryColumnSource(
        @NotNull final long[] data) {
        final ArrayBackedColumnSource<DBDateTime> result = new DateTimeArraySource();
        result.ensureCapacity(data.length);
        final WritableSource<Long> asLong = (WritableSource<Long>) result.reinterpret(long.class);
        try (final FillFromContext context = asLong.makeFillFromContext(data.length);
            final OrderedKeys range = OrderedKeys.forRange(0, data.length - 1)) {
            asLong.fillFromChunk(context, LongChunk.chunkWrap(data), range);
        }
        return result;
    }

    /**
     * Produces an ShortArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource.
     * @return an in-memory column source with the requested data
     */
    public static ArrayBackedColumnSource<Short> getMemoryColumnSource(
        @NotNull final short[] data) {
        final ArrayBackedColumnSource<Short> result = new ShortArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
            final OrderedKeys range = OrderedKeys.forRange(0, data.length - 1)) {
            result.fillFromChunk(context, ShortChunk.chunkWrap(data), range);
        }
        return result;
    }

    /**
     * Produces an empty ArrayBackedColumnSource with the given type and capacity.
     *
     * @param size the capacity of the returned column source
     * @param dataType the data type of the resultant column source
     * @param <T> the type parameter for the ColumnSource's type
     * @return an in-memory column source of the requested type
     */
    public static <T> ArrayBackedColumnSource<T> getMemoryColumnSource(final long size,
        @NotNull final Class<T> dataType) {
        return getMemoryColumnSource(size, dataType, null);
    }

    public static <T> ArrayBackedColumnSource<T> getMemoryColumnSource(
        @NotNull final Class<T> dataType, @Nullable final Class<?> componentType) {
        return getMemoryColumnSource(0, dataType, componentType);
    }

    /**
     * Produces an empty ArrayBackedColumnSource with the given type and capacity.
     *
     * @param size the capacity of the returned column source
     * @param dataType the data type of the resultant column source
     * @param componentType the component type for column sources of arrays or DbArrays
     * @param <T> the type parameter for the ColumnSource's type
     * @return an in-memory column source of the requested type
     */
    public static <T> ArrayBackedColumnSource<T> getMemoryColumnSource(final long size,
        @NotNull final Class<T> dataType, @Nullable final Class<?> componentType) {
        final ArrayBackedColumnSource<?> result;
        if (dataType == byte.class || dataType == Byte.class) {
            result = new ByteArraySource();
        } else if (dataType == char.class || dataType == Character.class) {
            result = new CharacterArraySource();
        } else if (dataType == double.class || dataType == Double.class) {
            result = new DoubleArraySource();
        } else if (dataType == float.class || dataType == Float.class) {
            result = new FloatArraySource();
        } else if (dataType == int.class || dataType == Integer.class) {
            result = new IntegerArraySource();
        } else if (dataType == long.class || dataType == Long.class) {
            result = new LongArraySource();
        } else if (dataType == short.class || dataType == Short.class) {
            result = new ShortArraySource();
        } else if (dataType == boolean.class || dataType == Boolean.class) {
            result = new BooleanArraySource();
        } else if (dataType == DBDateTime.class) {
            result = new DateTimeArraySource();
        } else {
            if (componentType != null) {
                result = new ObjectArraySource<>(dataType, componentType);
            } else {
                result = new ObjectArraySource<>(dataType);
            }
        }
        if (size > 0) {
            result.ensureCapacity(size);
        }
        // noinspection unchecked
        return (ArrayBackedColumnSource<T>) result;
    }

    @Override
    public abstract void ensureCapacity(long size, boolean nullFill);

    @Override
    public void shift(final long start, final long end, final long offset) {
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
     * Creates an in-memory ColumnSource from the supplied dataArray, using instanceof checks to
     * determine the appropriate type of column source to produce.
     *
     * @param dataArray the data to insert into the new column source
     * @return a ColumnSource with the supplied data.
     */
    public static WritableSource<?> getMemoryColumnSourceUntyped(@NotNull final Object dataArray) {
        return getMemoryColumnSourceUntyped(dataArray, dataArray.getClass().getComponentType(),
            dataArray.getClass().getComponentType().getComponentType());
    }

    /**
     * Creates an in-memory ColumnSource from the supplied dataArray, using instanceof checks to
     * determine the appropriate type of column source to produce.
     *
     * @param dataArray the data to insert into the new column source
     * @param dataType the data type of the resultant column source
     * @param componentType the component type for column sources of arrays or DbArrays
     * @return a ColumnSource with the supplied data.
     */
    public static <T> WritableSource<T> getMemoryColumnSourceUntyped(
        @NotNull final Object dataArray,
        @NotNull final Class<T> dataType,
        @Nullable final Class<?> componentType) {
        final WritableSource<?> result;
        if (dataArray instanceof boolean[]) {
            result = getMemoryColumnSource(ArrayUtils.getBoxedArray((boolean[]) dataArray),
                Boolean.class, null);
        } else if (dataArray instanceof byte[]) {
            result = getMemoryColumnSource((byte[]) dataArray);
        } else if (dataArray instanceof char[]) {
            result = getMemoryColumnSource((char[]) dataArray);
        } else if (dataArray instanceof double[]) {
            result = getMemoryColumnSource((double[]) dataArray);
        } else if (dataArray instanceof float[]) {
            result = getMemoryColumnSource((float[]) dataArray);
        } else if (dataArray instanceof int[]) {
            result = getMemoryColumnSource((int[]) dataArray);
        } else if (dataArray instanceof long[]) {
            result = getMemoryColumnSource((long[]) dataArray);
        } else if (dataArray instanceof short[]) {
            result = getMemoryColumnSource((short[]) dataArray);
        } else if (dataArray instanceof Boolean[]) {
            result = getMemoryColumnSource((Boolean[]) dataArray, Boolean.class, null);
        } else if (dataArray instanceof Byte[]) {
            result = getMemoryColumnSource(ArrayUtils.getUnboxedArray((Byte[]) dataArray));
        } else if (dataArray instanceof Character[]) {
            result = getMemoryColumnSource(ArrayUtils.getUnboxedArray((Character[]) dataArray));
        } else if (dataArray instanceof Double[]) {
            result = getMemoryColumnSource(ArrayUtils.getUnboxedArray((Double[]) dataArray));
        } else if (dataArray instanceof Float[]) {
            result = getMemoryColumnSource(ArrayUtils.getUnboxedArray((Float[]) dataArray));
        } else if (dataArray instanceof Integer[]) {
            result = getMemoryColumnSource(ArrayUtils.getUnboxedArray((Integer[]) dataArray));
        } else if (dataArray instanceof Long[]) {
            result = getMemoryColumnSource(ArrayUtils.getUnboxedArray((Long[]) dataArray));
        } else if (dataArray instanceof Short[]) {
            result = getMemoryColumnSource(ArrayUtils.getUnboxedArray((Short[]) dataArray));
        } else {
            // noinspection unchecked
            result = getMemoryColumnSource((T[]) dataArray, dataType, componentType);
        }
        // noinspection unchecked
        return (WritableSource<T>) result;
    }

    /**
     * Wrap the input array in an immutable {@link ColumnSource}. This method will unbox any boxed
     * values, and directly use the result array.
     *
     * @param dataArray The array to turn into a ColumnSource
     * @return An Immutable ColumnSource that directly wraps the input array.
     */
    public static ColumnSource<?> getImmutableMemoryColumnSource(@NotNull final Object dataArray) {
        final Class<?> arrayType = dataArray.getClass().getComponentType();
        if (arrayType == null) {
            throw new IllegalArgumentException(
                "Input value was not an array, was " + dataArray.getClass().getName());
        }

        return getImmutableMemoryColumnSource(dataArray, arrayType, arrayType.getComponentType());
    }

    /**
     * Wrap the input array in an immutable {@link ColumnSource}. This method will unbox any boxed
     * values, and directly use the result array. This version allows the user to specify the column
     * data type. It will automatically map column type Boolean/boolean with input array types
     * byte[] to {@link ImmutableBooleanArraySource} and columnType DBDateTime / array type long[]
     * to {@link ImmutableDateTimeArraySource}
     *
     * @param dataArray The array to turn into a ColumnSource
     * @param dataType the data type of the resultant column source
     * @param componentType the component type for column sources of arrays or DbArrays
     * @return An Immutable ColumnSource that directly wraps the input array.
     */
    public static <T> ColumnSource<T> getImmutableMemoryColumnSource(
        @NotNull final Object dataArray,
        @NotNull final Class<T> dataType,
        @Nullable final Class<?> componentType) {
        final ColumnSource<?> result;
        if (dataType == boolean.class) {
            result = (dataArray instanceof byte[])
                ? new ImmutableBooleanArraySource((byte[]) dataArray)
                : new ImmutableBooleanArraySource((boolean[]) dataArray);
        } else if (dataType == byte.class) {
            result = new ImmutableByteArraySource((byte[]) dataArray);
        } else if (dataType == char.class) {
            result = new ImmutableCharArraySource((char[]) dataArray);
        } else if (dataType == double.class) {
            result = new ImmutableDoubleArraySource((double[]) dataArray);
        } else if (dataType == float.class) {
            result = new ImmutableFloatArraySource((float[]) dataArray);
        } else if (dataType == int.class) {
            result = new ImmutableIntArraySource((int[]) dataArray);
        } else if (dataType == long.class) {
            result = new ImmutableLongArraySource((long[]) dataArray);
        } else if (dataType == short.class) {
            result = new ImmutableShortArraySource((short[]) dataArray);
        } else if (dataType == Boolean.class) {
            result = (dataArray instanceof byte[])
                ? new ImmutableBooleanArraySource((byte[]) dataArray)
                : new ImmutableBooleanArraySource((Boolean[]) dataArray);
        } else if (dataType == Byte.class) {
            result = new ImmutableByteArraySource(ArrayUtils.getUnboxedArray((Byte[]) dataArray));
        } else if (dataType == Character.class) {
            result =
                new ImmutableCharArraySource(ArrayUtils.getUnboxedArray((Character[]) dataArray));
        } else if (dataType == Double.class) {
            result =
                new ImmutableDoubleArraySource(ArrayUtils.getUnboxedArray((Double[]) dataArray));
        } else if (dataType == Float.class) {
            result = new ImmutableFloatArraySource(ArrayUtils.getUnboxedArray((Float[]) dataArray));
        } else if (dataType == Integer.class) {
            result = new ImmutableIntArraySource(ArrayUtils.getUnboxedArray((Integer[]) dataArray));
        } else if (dataType == Long.class) {
            result = new ImmutableLongArraySource(ArrayUtils.getUnboxedArray((Long[]) dataArray));
        } else if (dataType == Short.class) {
            result = new ImmutableShortArraySource(ArrayUtils.getUnboxedArray((Short[]) dataArray));
        } else if (dataType == DBDateTime.class && dataArray instanceof long[]) {
            result = new ImmutableDateTimeArraySource((long[]) dataArray);
        } else {
            // noinspection unchecked
            result = new ImmutableObjectArraySource<>((T[]) dataArray, dataType, componentType);
        }
        // noinspection unchecked
        return (ColumnSource<T>) result;
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    static int getBlockNo(final long from) {
        return (int) (from >> LOG_BLOCK_SIZE);
    }

    abstract Object getBlock(int blockIndex);

    abstract Object getPrevBlock(int blockIndex);

    @Override
    public void fillChunk(@NotNull final FillContext context,
        @NotNull final WritableChunk<? super Values> destination,
        @NotNull final OrderedKeys orderedKeys) {
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
                destination.copyFromArray(getBlock(fromBlock), fromOffsetInBlock,
                    destOffset.intValue(), sz);
                destOffset.add(sz);
            } else {
                final int sz = BLOCK_SIZE - fromOffsetInBlock;
                destination.copyFromArray(getBlock(fromBlock), fromOffsetInBlock,
                    destOffset.intValue(), sz);
                destOffset.add(sz);
                for (int blockNo = fromBlock + 1; blockNo < toBlock; ++blockNo) {
                    destination.copyFromArray(getBlock(blockNo), 0, destOffset.intValue(),
                        BLOCK_SIZE);
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
    public void fillChunkUnordered(@NotNull final FillContext context,
        @NotNull final WritableChunk<? super Values> destination,
        @NotNull final LongChunk<? extends KeyIndices> keyIndices) {
        fillSparseChunkUnordered(destination, keyIndices);
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull final FillContext context,
        @NotNull final WritableChunk<? super Values> destination,
        @NotNull final LongChunk<? extends KeyIndices> keyIndices) {
        fillSparsePrevChunkUnordered(destination, keyIndices);
    }

    /**
     * Resets the given chunk to provide a write-through reference to our backing array.
     * <p>
     * Note: This is unsafe to use if previous tracking has been enabled!
     *
     * @param chunk the writable chunk to reset to our backing array.
     * @param position position that we require
     * @return the first position addressable by the chunk
     */
    public abstract long resetWritableChunkToBackingStore(
        @NotNull final ResettableWritableChunk<?> chunk, long position);

    protected abstract void fillSparseChunk(@NotNull WritableChunk<? super Values> destination,
        @NotNull OrderedKeys indices);

    protected abstract void fillSparsePrevChunk(@NotNull WritableChunk<? super Values> destination,
        @NotNull OrderedKeys indices);

    protected abstract void fillSparseChunkUnordered(
        @NotNull WritableChunk<? super Values> destination,
        @NotNull LongChunk<? extends KeyIndices> indices);

    protected abstract void fillSparsePrevChunkUnordered(
        @NotNull WritableChunk<? super Values> destination,
        @NotNull LongChunk<? extends KeyIndices> indices);

    @Override
    public Chunk<Values> getChunk(@NotNull final GetContext context,
        @NotNull final OrderedKeys orderedKeys) {
        final LongChunk<OrderedKeyRanges> ranges = orderedKeys.asKeyRangesChunk();
        if (ranges.size() == 2) {
            final long first = ranges.get(0);
            final long last = ranges.get(1);

            if (getBlockNo(first) == getBlockNo(last)) {
                // Optimization if the caller requests a single range which happens to fall within a
                // single block
                final int blockPos = getBlockNo(first);
                return DefaultGetContext.resetChunkFromArray(context,
                    getBlock(blockPos), (int) (first & INDEX_MASK), (int) (last - first + 1));
            }
        }

        return getChunkByFilling(context, orderedKeys);
    }

    private static class ArrayAdapter<T> implements Array.Visitor, PrimitiveArray.Visitor {
        private ArrayBackedColumnSource<?> out;

        public ArrayBackedColumnSource<?> getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(PrimitiveArray<?> primitive) {
            primitive.walk((PrimitiveArray.Visitor) this);
        }

        @Override
        public void visit(ByteArray byteArray) {
            out = ArrayBackedColumnSource.getMemoryColumnSource(byteArray.values());
        }

        @Override
        public void visit(BooleanArray booleanArray) {
            out = ArrayBackedColumnSource.getBooleanMemoryColumnSource(booleanArray.values());
        }

        @Override
        public void visit(CharArray charArray) {
            out = ArrayBackedColumnSource.getMemoryColumnSource(charArray.values());
        }

        @Override
        public void visit(ShortArray shortArray) {
            out = ArrayBackedColumnSource.getMemoryColumnSource(shortArray.values());
        }

        @Override
        public void visit(IntArray intArray) {
            out = ArrayBackedColumnSource.getMemoryColumnSource(intArray.values());
        }

        @Override
        public void visit(LongArray longArray) {
            out = ArrayBackedColumnSource.getMemoryColumnSource(longArray.values());
        }

        @Override
        public void visit(FloatArray floatArray) {
            out = ArrayBackedColumnSource.getMemoryColumnSource(floatArray.values());
        }

        @Override
        public void visit(DoubleArray doubleArray) {
            out = ArrayBackedColumnSource.getMemoryColumnSource(doubleArray.values());
        }

        @Override
        public void visit(GenericArray<?> generic) {
            generic.componentType().walk(new Visitor() {
                @Override
                public void visit(StringType stringType) {
                    out = ArrayBackedColumnSource.getMemoryColumnSource(
                        generic.cast(stringType).values(), String.class, null);
                }

                @Override
                public void visit(InstantType instantType) {
                    DateTimeArraySource source = new DateTimeArraySource();
                    source.ensureCapacity(generic.size());
                    int ix = 0;
                    for (Instant value : generic.cast(instantType).values()) {
                        if (value == null) {
                            source.set(ix++, NULL_LONG);
                        } else {
                            long nanos = Math.addExact(
                                TimeUnit.SECONDS.toNanos(value.getEpochSecond()), value.getNano());
                            source.set(ix++, nanos);
                        }
                    }
                    out = source;
                }

                @Override
                public void visit(ArrayType<?, ?> arrayType) {
                    // noinspection unchecked
                    ArrayType<T, ?> tType = (ArrayType<T, ?>) arrayType;
                    out =
                        ArrayBackedColumnSource.getMemoryColumnSource(generic.cast(tType).values(),
                            tType.clazz(), arrayType.componentType().clazz());
                }

                @Override
                public void visit(CustomType<?> customType) {
                    // noinspection unchecked
                    CustomType<T> tType = (CustomType<T>) customType;
                    out = ArrayBackedColumnSource
                        .getMemoryColumnSource(generic.cast(tType).values(), tType.clazz(), null);
                }
            });
        }
    }
}
