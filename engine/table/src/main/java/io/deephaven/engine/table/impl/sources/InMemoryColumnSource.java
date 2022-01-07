package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.flat.*;
import io.deephaven.engine.table.impl.sources.immutable.*;
import io.deephaven.time.DateTime;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This is a marker interface for a column source that is entirely within memory; therefore select operations should not
 * try to copy it into memory a second time.
 */
public interface InMemoryColumnSource {
    /**
     * Create an immutable in-memory column source of the provided size with an array allocated of the appropriate size
     *
     * @param longSize the size of the array to allocate
     * @param dataType the data type of the resultant column source
     * @param componentType the component type for column sources of arrays or Vectors
     * @return An Immutable ColumnSource that directly wraps the input array.
     */
    static <T> WritableColumnSource<T> getFlatMemoryColumnSource(long longSize,
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType) {
        // There is an jdk.internal.util.ArraysSupport.MAX_ARRAY_LENGTH we would like to use, but I cannot compile that
        // way
        if (longSize > Integer.MAX_VALUE - 8) {
            return makeFlat2DSource(dataType, componentType);
        }
        return makeFlatSource(longSize, dataType, componentType);
    }

    @NotNull
    static <T> WritableColumnSource<T> makeFlatSource(long longSize, @NotNull Class<T> dataType,
            @Nullable Class<?> componentType) {
        final WritableColumnSource<?> result;
        if (dataType == boolean.class || dataType == Boolean.class) {
            result = new WritableByteAsBooleanColumnSource(new FlatByteArraySource());
        } else if (dataType == char.class || dataType == Character.class) {
            result = new FlatCharArraySource();
        } else if (dataType == byte.class || dataType == Byte.class) {
            result = new FlatByteArraySource();
        } else if (dataType == double.class || dataType == Double.class) {
            result = new FlatDoubleArraySource();
        } else if (dataType == float.class || dataType == Float.class) {
            result = new FlatFloatArraySource();
        } else if (dataType == int.class || dataType == Integer.class) {
            result = new FlatIntArraySource();
        } else if (dataType == long.class || dataType == Long.class) {
            result = new FlatLongArraySource();
        } else if (dataType == short.class || dataType == Short.class) {
            result = new FlatShortArraySource();
        } else if (dataType == DateTime.class) {
            result = new WritableLongAsDateTimeColumnSource(new FlatLongArraySource());
        } else {
            result = new FlatObjectArraySource<>(dataType, componentType);
        }
        // noinspection unchecked
        return (WritableColumnSource<T>) result;
    }

    @NotNull
    static <T> WritableColumnSource<T> makeFlat2DSource(@NotNull Class<T> dataType,
            @Nullable Class<?> componentType) {
        final WritableColumnSource<?> result;
        if (dataType == boolean.class || dataType == Boolean.class) {
            result = new WritableByteAsBooleanColumnSource(new Flat2DByteArraySource());
        } else if (dataType == char.class || dataType == Character.class) {
            result = new Flat2DCharArraySource();
        } else if (dataType == byte.class || dataType == Byte.class) {
            result = new Flat2DByteArraySource();
        } else if (dataType == double.class || dataType == Double.class) {
            result = new Flat2DDoubleArraySource();
        } else if (dataType == float.class || dataType == Float.class) {
            result = new Flat2DFloatArraySource();
        } else if (dataType == int.class || dataType == Integer.class) {
            result = new Flat2DIntArraySource();
        } else if (dataType == long.class || dataType == Long.class) {
            result = new Flat2DLongArraySource();
        } else if (dataType == short.class || dataType == Short.class) {
            result = new Flat2DShortArraySource();
        } else if (dataType == DateTime.class) {
            result = new WritableLongAsDateTimeColumnSource(new Flat2DLongArraySource());
        } else {
            result = new Flat2DObjectArraySource<>(dataType, componentType);
        }
        // noinspection unchecked
        return (WritableColumnSource<T>) result;
    }

    /**
     * Wrap the input array in an immutable {@link ColumnSource}. This method will unbox any boxed values, and directly
     * use the result array.
     *
     * @param dataArray The array to turn into a ColumnSource
     * @return An Immutable ColumnSource that directly wraps the input array.
     */
    static ColumnSource<?> getImmutableMemoryColumnSource(@NotNull final Object dataArray) {
        final Class<?> arrayType = dataArray.getClass().getComponentType();
        if (arrayType == null) {
            throw new IllegalArgumentException("Input value was not an array, was " + dataArray.getClass().getName());
        }

        return getImmutableMemoryColumnSource(dataArray, arrayType, arrayType.getComponentType());
    }

    /**
     * Wrap the input array in an immutable {@link ColumnSource}. This method will unbox any boxed values, and directly
     * use the result array. This version allows the user to specify the column data type. It will automatically map
     * column type Boolean/boolean with input array types byte[] and columnType DateTime / array type long[].
     *
     * @param dataArray The array to turn into a ColumnSource
     * @param dataType the data type of the resultant column source
     * @param componentType the component type for column sources of arrays or Vectors
     * @return An Immutable ColumnSource that directly wraps the input array.
     */
    static <T> ColumnSource<T> getImmutableMemoryColumnSource(@NotNull final Object dataArray,
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType) {
        final ColumnSource<?> result;
        if (dataType == boolean.class || dataType == Boolean.class) {
            if (dataArray instanceof byte[]) {
                result = new ByteAsBooleanColumnSource(new FlatByteArraySource((byte[]) dataArray));
            } else if (dataArray instanceof boolean[]) {
                result = new ByteAsBooleanColumnSource(
                        new FlatByteArraySource(BooleanUtils.booleanAsByte((boolean[]) dataArray)));
            } else if (dataArray instanceof Boolean[]) {
                result = new ByteAsBooleanColumnSource(
                        new FlatByteArraySource(BooleanUtils.booleanAsByte((Boolean[]) dataArray)));
            } else {
                throw new IllegalArgumentException("Invalid dataArray for type " + dataType);
            }
        } else if (dataType == byte.class) {
            result = new FlatByteArraySource((byte[]) dataArray);
        } else if (dataType == char.class) {
            result = new FlatCharArraySource((char[]) dataArray);
        } else if (dataType == double.class) {
            result = new FlatDoubleArraySource((double[]) dataArray);
        } else if (dataType == float.class) {
            result = new FlatFloatArraySource((float[]) dataArray);
        } else if (dataType == int.class) {
            result = new FlatIntArraySource((int[]) dataArray);
        } else if (dataType == long.class) {
            result = new FlatLongArraySource((long[]) dataArray);
        } else if (dataType == short.class) {
            result = new FlatShortArraySource((short[]) dataArray);
        } else if (dataType == Byte.class) {
            result = new FlatByteArraySource(ArrayTypeUtils.getUnboxedArray((Byte[]) dataArray));
        } else if (dataType == Character.class) {
            result = new FlatCharArraySource(ArrayTypeUtils.getUnboxedArray((Character[]) dataArray));
        } else if (dataType == Double.class) {
            result = new FlatDoubleArraySource(ArrayTypeUtils.getUnboxedArray((Double[]) dataArray));
        } else if (dataType == Float.class) {
            result = new FlatFloatArraySource(ArrayTypeUtils.getUnboxedArray((Float[]) dataArray));
        } else if (dataType == Integer.class) {
            result = new FlatIntArraySource(ArrayTypeUtils.getUnboxedArray((Integer[]) dataArray));
        } else if (dataType == Long.class) {
            result = new FlatLongArraySource(ArrayTypeUtils.getUnboxedArray((Long[]) dataArray));
        } else if (dataType == Short.class) {
            result = new FlatShortArraySource(ArrayTypeUtils.getUnboxedArray((Short[]) dataArray));
        } else if (dataType == DateTime.class && dataArray instanceof long[]) {
            result = new LongAsDateTimeColumnSource(new FlatLongArraySource((long[]) dataArray));
        } else {
            // noinspection unchecked
            result = new FlatObjectArraySource<>(dataType, componentType, (T[]) dataArray);
        }
        // noinspection unchecked
        return (ColumnSource<T>) result;
    }
}
