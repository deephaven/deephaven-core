/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.*;
import io.deephaven.engine.table.impl.sources.immutable.Immutable2DCharArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableCharArraySource;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This is a marker interface for a column source that is entirely within memory; therefore select operations should not
 * try to copy it into memory a second time.
 */
public interface InMemoryColumnSource {
    // We would like to use jdk.internal.util.ArraysSupport.MAX_ARRAY_LENGTH, but it is not exported
    int TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD = Integer.MAX_VALUE - 8;

    /**
     * Create an immutable in-memory column source that is capable of holding longSize elements.
     *
     * Note, that the backing array may not be allocated after this call; you still must call
     * {@link WritableColumnSource#ensureCapacity(long)}.
     *
     * @param longSize the minimum required size that the column source must support
     * @param dataType the data type of the resultant column source
     * @param componentType the component type for column sources of arrays or Vectors
     * @return an immutable WritableColumnSource
     */
    static <T> WritableColumnSource<T> getImmutableMemoryColumnSource(long longSize,
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType) {
        if (longSize > TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD) {
            return makeImmutable2DSource(dataType, componentType);
        }
        return makeImmutableSource(dataType, componentType);
    }

    @NotNull
    static <T> WritableColumnSource<T> makeImmutableSource(@NotNull Class<T> dataType,
            @Nullable Class<?> componentType) {
        final WritableColumnSource<?> result;
        if (dataType == boolean.class || dataType == Boolean.class) {
            result = new WritableByteAsBooleanColumnSource(new ImmutableByteArraySource());
        } else if (dataType == char.class || dataType == Character.class) {
            result = new ImmutableCharArraySource();
        } else if (dataType == byte.class || dataType == Byte.class) {
            result = new ImmutableByteArraySource();
        } else if (dataType == double.class || dataType == Double.class) {
            result = new ImmutableDoubleArraySource();
        } else if (dataType == float.class || dataType == Float.class) {
            result = new ImmutableFloatArraySource();
        } else if (dataType == int.class || dataType == Integer.class) {
            result = new ImmutableIntArraySource();
        } else if (dataType == long.class || dataType == Long.class) {
            result = new ImmutableLongArraySource();
        } else if (dataType == short.class || dataType == Short.class) {
            result = new ImmutableShortArraySource();
        } else if (dataType == DateTime.class) {
            result = new WritableLongAsDateTimeColumnSource(new ImmutableLongArraySource());
        } else {
            result = new ImmutableObjectArraySource<>(dataType, componentType);
        }
        // noinspection unchecked
        return (WritableColumnSource<T>) result;
    }

    @NotNull
    static <T> WritableColumnSource<T> makeImmutable2DSource(@NotNull Class<T> dataType,
            @Nullable Class<?> componentType) {
        final WritableColumnSource<?> result;
        if (dataType == boolean.class || dataType == Boolean.class) {
            result = new WritableByteAsBooleanColumnSource(new Immutable2DByteArraySource());
        } else if (dataType == char.class || dataType == Character.class) {
            result = new Immutable2DCharArraySource();
        } else if (dataType == byte.class || dataType == Byte.class) {
            result = new Immutable2DByteArraySource();
        } else if (dataType == double.class || dataType == Double.class) {
            result = new Immutable2DDoubleArraySource();
        } else if (dataType == float.class || dataType == Float.class) {
            result = new Immutable2DFloatArraySource();
        } else if (dataType == int.class || dataType == Integer.class) {
            result = new Immutable2DIntArraySource();
        } else if (dataType == long.class || dataType == Long.class) {
            result = new Immutable2DLongArraySource();
        } else if (dataType == short.class || dataType == Short.class) {
            result = new Immutable2DShortArraySource();
        } else if (dataType == DateTime.class) {
            result = new WritableLongAsDateTimeColumnSource(new Immutable2DLongArraySource());
        } else {
            result = new Immutable2DObjectArraySource<>(dataType, componentType);
        }
        // noinspection unchecked
        return (WritableColumnSource<T>) result;
    }

    @NotNull
    static <T> ColumnSource<T> makeImmutableConstantSource(
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType,
            @Nullable final T value) {
        final ColumnSource<?> result;
        if (dataType == boolean.class || dataType == Boolean.class) {
            result = new ByteAsBooleanColumnSource(
                    new ImmutableConstantByteSource(BooleanUtils.booleanAsByte((Boolean) value)));
        } else if (dataType == char.class || dataType == Character.class) {
            result = new ImmutableConstantCharSource(TypeUtils.unbox((Character) value));
        } else if (dataType == byte.class || dataType == Byte.class) {
            result = new ImmutableConstantByteSource(TypeUtils.unbox((Byte) value));
        } else if (dataType == double.class || dataType == Double.class) {
            result = new ImmutableConstantDoubleSource(TypeUtils.unbox((Double) value));
        } else if (dataType == float.class || dataType == Float.class) {
            result = new ImmutableConstantFloatSource(TypeUtils.unbox((Float) value));
        } else if (dataType == int.class || dataType == Integer.class) {
            result = new ImmutableConstantIntSource(TypeUtils.unbox((Integer) value));
        } else if (dataType == long.class || dataType == Long.class) {
            result = new ImmutableConstantLongSource(TypeUtils.unbox((Long) value));
        } else if (dataType == short.class || dataType == Short.class) {
            result = new ImmutableConstantShortSource(TypeUtils.unbox((Short) value));
        } else if (dataType == DateTime.class) {
            result = new LongAsDateTimeColumnSource(
                    new ImmutableConstantLongSource(DateTimeUtils.nanos((DateTime) value)));
        } else {
            result = new ImmutableConstantObjectSource<>(dataType, componentType, value);
        }
        // noinspection unchecked
        return (ColumnSource<T>) result;
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
                result = new ByteAsBooleanColumnSource(new ImmutableByteArraySource((byte[]) dataArray));
            } else if (dataArray instanceof boolean[]) {
                result = new ByteAsBooleanColumnSource(
                        new ImmutableByteArraySource(BooleanUtils.booleanAsByteArray((boolean[]) dataArray)));
            } else if (dataArray instanceof Boolean[]) {
                result = new ByteAsBooleanColumnSource(
                        new ImmutableByteArraySource(BooleanUtils.booleanAsByteArray((Boolean[]) dataArray)));
            } else {
                throw new IllegalArgumentException("Invalid dataArray for type " + dataType);
            }
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
        } else if (dataType == Byte.class) {
            result = new ImmutableByteArraySource(ArrayTypeUtils.getUnboxedArray((Byte[]) dataArray));
        } else if (dataType == Character.class) {
            result = new ImmutableCharArraySource(ArrayTypeUtils.getUnboxedArray((Character[]) dataArray));
        } else if (dataType == Double.class) {
            result = new ImmutableDoubleArraySource(ArrayTypeUtils.getUnboxedArray((Double[]) dataArray));
        } else if (dataType == Float.class) {
            result = new ImmutableFloatArraySource(ArrayTypeUtils.getUnboxedArray((Float[]) dataArray));
        } else if (dataType == Integer.class) {
            result = new ImmutableIntArraySource(ArrayTypeUtils.getUnboxedArray((Integer[]) dataArray));
        } else if (dataType == Long.class) {
            result = new ImmutableLongArraySource(ArrayTypeUtils.getUnboxedArray((Long[]) dataArray));
        } else if (dataType == Short.class) {
            result = new ImmutableShortArraySource(ArrayTypeUtils.getUnboxedArray((Short[]) dataArray));
        } else if (dataType == DateTime.class && dataArray instanceof long[]) {
            result = new LongAsDateTimeColumnSource(new ImmutableLongArraySource((long[]) dataArray));
        } else {
            // noinspection unchecked
            result = new ImmutableObjectArraySource<>(dataType, componentType, (T[]) dataArray);
        }
        // noinspection unchecked
        return (ColumnSource<T>) result;
    }
}
