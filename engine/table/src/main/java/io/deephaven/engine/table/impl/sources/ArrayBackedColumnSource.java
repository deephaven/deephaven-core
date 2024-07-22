//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;

/**
 * A ColumnSource backed by in-memory arrays of data.
 * <p>
 * The column source is dense with keys from 0 to capacity, there can be no holes. Arrays are divided into blocks so
 * that the column source can be incrementally expanded without copying data from one array to another.
 */
public abstract class ArrayBackedColumnSource<T>
        extends AbstractColumnSource<T>
        implements FillUnordered<Values>, WritableColumnSource<T>, InMemoryColumnSource,
        ChunkedBackingStoreExposedWritableSource {

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

    // The inUse calculations are confusing because there are actually three levels of indexing (where the third level
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

    static final SoftRecycler<long[]> inUseRecycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new long[IN_USE_BLOCK_SIZE],
            block -> Arrays.fill(block, 0));

    public static WritableColumnSource<?> from(Array<?> array) {
        return array.walk(new ArrayAdapter<>());
    }

    public static <T> WritableColumnSource<T> from(PrimitiveArray<T> array) {
        PrimitiveArray.Visitor<WritableColumnSource<?>> adapter = new ArrayAdapter<>();
        // noinspection unchecked
        return (WritableColumnSource<T>) array.walk(adapter);
    }

    /**
     * The highest slot that can be used without a call to {@link #ensureCapacity(long)}.
     */
    long maxIndex;

    /**
     * A bitset with the same two-level structure as the array-backed data, except that the inner array is interpreted
     * as a bitset (and is thus not very big... its length is blockSize / 64, and because blockSize is currently 256,
     * the size of the inner array is 4).
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
    public static <T> WritableColumnSource<T> getMemoryColumnSource(@NotNull final Collection<T> data,
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType) {
        final WritableColumnSource<T> result = getMemoryColumnSource(data.size(), dataType, componentType);
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
    public static <T> WritableColumnSource<T> getMemoryColumnSource(@NotNull final T[] data,
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType) {
        final WritableColumnSource<T> result = getMemoryColumnSource(data.length, dataType, componentType);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
                final RowSequence range = RowSequenceFactory.forRange(0, data.length - 1)) {
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
                final RowSequence range = RowSequenceFactory.forRange(0, data.length - 1)) {
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
    public static ArrayBackedColumnSource<Boolean> getBooleanMemoryColumnSource(@NotNull final byte[] data) {
        final ArrayBackedColumnSource<Boolean> result = new BooleanArraySource();
        final WritableColumnSource<Byte> dest = (WritableColumnSource<Byte>) result.reinterpret(byte.class);
        result.ensureCapacity(data.length);
        try (final FillFromContext context = dest.makeFillFromContext(data.length);
                final RowSequence range = RowSequenceFactory.forRange(0, data.length - 1)) {
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
    public static ArrayBackedColumnSource<Character> getMemoryColumnSource(@NotNull final char[] data) {
        final ArrayBackedColumnSource<Character> result = new CharacterArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
                final RowSequence range = RowSequenceFactory.forRange(0, data.length - 1)) {
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
    public static ArrayBackedColumnSource<Double> getMemoryColumnSource(@NotNull final double[] data) {
        final ArrayBackedColumnSource<Double> result = new DoubleArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
                final RowSequence range = RowSequenceFactory.forRange(0, data.length - 1)) {
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
    public static ArrayBackedColumnSource<Float> getMemoryColumnSource(@NotNull final float[] data) {
        final ArrayBackedColumnSource<Float> result = new FloatArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
                final RowSequence range = RowSequenceFactory.forRange(0, data.length - 1)) {
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
    public static ArrayBackedColumnSource<Integer> getMemoryColumnSource(@NotNull final int[] data) {
        final ArrayBackedColumnSource<Integer> result = new IntegerArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
                final RowSequence range = RowSequenceFactory.forRange(0, data.length - 1)) {
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
                final RowSequence range = RowSequenceFactory.forRange(0, data.length - 1)) {
            result.fillFromChunk(context, LongChunk.chunkWrap(data), range);
        }
        return result;
    }

    /**
     * Produces an InstantArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource, represented as long nanoseconds since
     *        the epoch
     * @return an in-memory column source with the requested data
     */
    public static WritableColumnSource<Instant> getInstantMemoryColumnSource(LongChunk<Values> data) {
        final WritableColumnSource<Instant> result = new InstantArraySource();
        result.ensureCapacity(data.size());
        for (int ii = 0; ii < data.size(); ++ii) {
            result.set(ii, data.get(ii));
        }
        return result;
    }

    /**
     * Produces an InstantArraySource with the given data.
     *
     * @param data an array containing the data to insert into the ColumnSource, represented as long nanoseconds since
     *        the epoch
     * @return an in-memory column source with the requested data
     */
    public static WritableColumnSource<Instant> getInstantMemoryColumnSource(@NotNull final long[] data) {
        final WritableColumnSource<Instant> result = new InstantArraySource();
        result.ensureCapacity(data.length);
        final WritableColumnSource<Long> asLong = (WritableColumnSource<Long>) result.reinterpret(long.class);
        try (final FillFromContext context = asLong.makeFillFromContext(data.length);
                final RowSequence range = RowSequenceFactory.forRange(0, data.length - 1)) {
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
    public static ArrayBackedColumnSource<Short> getMemoryColumnSource(@NotNull final short[] data) {
        final ArrayBackedColumnSource<Short> result = new ShortArraySource();
        result.ensureCapacity(data.length);
        try (final FillFromContext context = result.makeFillFromContext(data.length);
                final RowSequence range = RowSequenceFactory.forRange(0, data.length - 1)) {
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
    public static <T> WritableColumnSource<T> getMemoryColumnSource(final long size,
            @NotNull final Class<T> dataType) {
        return getMemoryColumnSource(size, dataType, null);
    }

    public static <T> WritableColumnSource<T> getMemoryColumnSource(@NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType) {
        return getMemoryColumnSource(0, dataType, componentType);
    }

    /**
     * Produces an empty ArrayBackedColumnSource with the given type and capacity.
     *
     * @param size the capacity of the returned column source
     * @param dataType the data type of the resultant column source
     * @param componentType the component type for column sources of arrays or Vectors
     * @param <T> the type parameter for the ColumnSource's type
     * @return an in-memory column source of the requested type
     */
    public static <T> WritableColumnSource<T> getMemoryColumnSource(final long size,
            @NotNull final Class<T> dataType, @Nullable final Class<?> componentType) {
        final WritableColumnSource<?> result;
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
        } else if (dataType == Instant.class) {
            result = new InstantArraySource();
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
        return (WritableColumnSource<T>) result;
    }

    @Override
    public abstract void ensureCapacity(long size, boolean nullFill);

    /**
     * Creates an in-memory ColumnSource from the supplied dataArray, using instanceof checks to determine the
     * appropriate type of column source to produce.
     *
     * @param dataArray the data to insert into the new column source
     * @return a ColumnSource with the supplied data.
     */
    public static WritableColumnSource<?> getMemoryColumnSourceUntyped(@NotNull final Object dataArray) {
        return getMemoryColumnSourceUntyped(dataArray, dataArray.getClass().getComponentType(),
                dataArray.getClass().getComponentType().getComponentType());
    }

    /**
     * Creates an in-memory ColumnSource from the supplied dataArray, using instanceof checks to determine the
     * appropriate type of column source to produce.
     *
     * @param dataArray the data to insert into the new column source
     * @param dataType the data type of the resultant column source
     * @param componentType the component type for column sources of arrays or Vectors
     * @return a ColumnSource with the supplied data.
     */
    public static <T> WritableColumnSource<T> getMemoryColumnSourceUntyped(@NotNull final Object dataArray,
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType) {
        final WritableColumnSource<?> result;
        if (dataArray instanceof boolean[]) {
            result = getMemoryColumnSource(ArrayTypeUtils.getBoxedArray((boolean[]) dataArray), Boolean.class, null);
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
            result = getMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Byte[]) dataArray));
        } else if (dataArray instanceof Character[]) {
            result = getMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Character[]) dataArray));
        } else if (dataArray instanceof Double[]) {
            result = getMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Double[]) dataArray));
        } else if (dataArray instanceof Float[]) {
            result = getMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Float[]) dataArray));
        } else if (dataArray instanceof Integer[]) {
            result = getMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Integer[]) dataArray));
        } else if (dataArray instanceof Long[]) {
            result = getMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Long[]) dataArray));
        } else if (dataArray instanceof Short[]) {
            result = getMemoryColumnSource(ArrayTypeUtils.getUnboxedArray((Short[]) dataArray));
        } else {
            // noinspection unchecked
            result = getMemoryColumnSource((T[]) dataArray, dataType, componentType);
        }
        // noinspection unchecked
        return (WritableColumnSource<T>) result;
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
    public void fillChunkUnordered(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final LongChunk<? extends RowKeys> keyIndices) {
        fillSparseChunkUnordered(destination, keyIndices);
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final LongChunk<? extends RowKeys> keyIndices) {
        fillSparsePrevChunkUnordered(destination, keyIndices);
    }

    protected abstract void fillSparseChunk(@NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence indices);

    protected abstract void fillSparsePrevChunk(@NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence indices);

    protected abstract void fillSparseChunkUnordered(@NotNull WritableChunk<? super Values> destination,
            @NotNull LongChunk<? extends RowKeys> indices);

    protected abstract void fillSparsePrevChunkUnordered(@NotNull WritableChunk<? super Values> destination,
            @NotNull LongChunk<? extends RowKeys> indices);

    @Override
    public Chunk<Values> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (rowSequence.isContiguous() && !rowSequence.isEmpty()) {
            final long first = rowSequence.firstRowKey();
            final long last = rowSequence.lastRowKey();

            if (getBlockNo(first) == getBlockNo(last)) {
                // Optimization if the caller requests a single range which happens to fall within a single block
                final int blockPos = getBlockNo(first);
                return DefaultGetContext.resetChunkFromArray(context,
                        getBlock(blockPos), (int) (first & INDEX_MASK), (int) (last - first + 1));
            }
        }

        return getChunkByFilling(context, rowSequence);
    }

    private static class ArrayAdapter<T>
            implements Array.Visitor<WritableColumnSource<?>>, PrimitiveArray.Visitor<WritableColumnSource<?>> {

        @Override
        public WritableColumnSource<?> visit(PrimitiveArray<?> primitive) {
            return primitive.walk((PrimitiveArray.Visitor<WritableColumnSource<?>>) this);
        }

        @Override
        public WritableColumnSource<?> visit(ByteArray byteArray) {
            return ArrayBackedColumnSource.getMemoryColumnSource(byteArray.values());
        }

        @Override
        public WritableColumnSource<?> visit(BooleanArray booleanArray) {
            return ArrayBackedColumnSource.getBooleanMemoryColumnSource(booleanArray.values());
        }

        @Override
        public WritableColumnSource<?> visit(CharArray charArray) {
            return ArrayBackedColumnSource.getMemoryColumnSource(charArray.values());
        }

        @Override
        public WritableColumnSource<?> visit(ShortArray shortArray) {
            return ArrayBackedColumnSource.getMemoryColumnSource(shortArray.values());
        }

        @Override
        public WritableColumnSource<?> visit(IntArray intArray) {
            return ArrayBackedColumnSource.getMemoryColumnSource(intArray.values());
        }

        @Override
        public WritableColumnSource<?> visit(LongArray longArray) {
            return ArrayBackedColumnSource.getMemoryColumnSource(longArray.values());
        }

        @Override
        public WritableColumnSource<?> visit(FloatArray floatArray) {
            return ArrayBackedColumnSource.getMemoryColumnSource(floatArray.values());
        }

        @Override
        public WritableColumnSource<?> visit(DoubleArray doubleArray) {
            return ArrayBackedColumnSource.getMemoryColumnSource(doubleArray.values());
        }

        @Override
        public WritableColumnSource<?> visit(GenericArray<?> generic) {
            return generic.componentType().walk(new Visitor<>() {
                @Override
                public WritableColumnSource<?> visit(BoxedType<?> boxedType) {
                    return simple(boxedType);
                }

                @Override
                public WritableColumnSource<?> visit(StringType stringType) {
                    return simple(stringType);
                }

                @Override
                public WritableColumnSource<?> visit(InstantType instantType) {
                    return simple(instantType);
                }

                @Override
                public WritableColumnSource<?> visit(ArrayType<?, ?> arrayType) {
                    // noinspection unchecked
                    ArrayType<T, ?> tType = (ArrayType<T, ?>) arrayType;
                    return ArrayBackedColumnSource.getMemoryColumnSource(
                            generic.cast(tType).values(), tType.clazz(), arrayType.componentType().clazz());
                }

                @Override
                public WritableColumnSource<?> visit(CustomType<?> customType) {
                    return simple(customType);
                }

                private <X> WritableColumnSource<X> simple(GenericType<X> type) {
                    return ArrayBackedColumnSource.getMemoryColumnSource(generic.cast(type).values(), type.clazz(),
                            null);
                }
            });
        }
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }
}
