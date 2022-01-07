package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.flat.*;
import io.deephaven.time.DateTime;
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
        final int size = Math.toIntExact(longSize);
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
}
