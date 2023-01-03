/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;

public class ReinterpretUtils {

    /**
     * Given a DateTime column source turn it into a long column source, either via reinterpretation or wrapping.
     *
     * @param source the source to turn into a long source
     *
     * @return the long source
     */
    public static ColumnSource<Long> dateTimeToLongSource(ColumnSource<?> source) {
        if (source.allowsReinterpret(long.class)) {
            return source.reinterpret(long.class);
        } else {
            // noinspection unchecked
            return new DateTimeAsLongColumnSource((ColumnSource<DateTime>) source);
        }
    }

    /**
     * Given a long column source turn it into a DateTime column source, either via reinterpretation or wrapping.
     *
     * @param source the source to turn into a DateTime source
     *
     * @return the long source
     */
    public static ColumnSource<DateTime> longToDateTimeSource(ColumnSource<?> source) {
        if (source.allowsReinterpret(DateTime.class)) {
            return source.reinterpret(DateTime.class);
        } else {
            // noinspection unchecked
            return new LongAsDateTimeColumnSource((ColumnSource<Long>) source);
        }
    }

    /**
     * Given a Boolean column source turn it into a byte column source, either via reinterpretation or wrapping.
     *
     * @param source the source to turn into a byte source
     *
     * @return the byte source
     */
    public static ColumnSource<Byte> booleanToByteSource(ColumnSource<?> source) {
        if (source.allowsReinterpret(byte.class)) {
            return source.reinterpret(byte.class);
        } else {
            // noinspection unchecked
            return new BooleanAsByteColumnSource((ColumnSource<Boolean>) source);
        }
    }

    /**
     * If source is something that we prefer to handle as a primitive, do the appropriate conversion.
     *
     * @param source The source to convert
     * @return If possible, the source converted to a primitive, otherwise the source
     */
    public static ColumnSource<?> maybeConvertToPrimitive(ColumnSource<?> source) {
        if (source.getType() == Boolean.class || source.getType() == boolean.class) {
            return booleanToByteSource(source);
        }
        if (source.getType() == DateTime.class) {
            return dateTimeToLongSource(source);
        }
        return source;
    }

    /**
     * If {@code dataType} is something that we prefer to handle as a primitive, emit the appropriate {@link ChunkType},
     * else the normal ChunkType for the data type.
     *
     * @param dataType The data type to convert to a {@link ChunkType}
     * @return The appropriate {@link ChunkType} to use when extracting primitives from the source
     */
    public static ChunkType maybeConvertToPrimitiveChunkType(@NotNull final Class<?> dataType) {
        if (dataType == Boolean.class || dataType == boolean.class) {
            return ChunkType.Byte;
        }
        if (dataType == DateTime.class) {
            return ChunkType.Long;
        }
        return ChunkType.fromElementType(dataType);
    }

    /**
     * If {@code dataType} is something that we prefer to handle as a primitive, emit the appropriate {@link Class data
     * type to use}, else return {@code dataType}.
     *
     * @param dataType The data type to examine
     * @return The appropriate data type to use when extracting primitives from the source
     */
    public static Class<?> maybeConvertToPrimitiveDataType(@NotNull final Class<?> dataType) {
        if (dataType == Boolean.class || dataType == boolean.class) {
            return byte.class;
        }
        if (dataType == DateTime.class) {
            return long.class;
        }
        return dataType;
    }

    /**
     * Reinterpret or box {@link ColumnSource} back to its original type.
     *
     * @param originalType The type to convert to
     * @param source The source to convert
     * @return Reinterpret or box source back to the original type if possible
     */
    public static ColumnSource<?> convertToOriginal(@NotNull final Class<?> originalType,
            @NotNull final ColumnSource<?> source) {
        if (originalType == Boolean.class) {
            if (source.getType() != byte.class) {
                throw new UnsupportedOperationException(
                        "Cannot convert column of type " + source.getType() + " to Boolean");
            }
            // noinspection unchecked
            return source.allowsReinterpret(Boolean.class) ? source.reinterpret(Boolean.class)
                    : new BoxedColumnSource.OfBoolean((ColumnSource<Byte>) source);
        }
        if (originalType == DateTime.class) {
            if (source.getType() != long.class) {
                throw new UnsupportedOperationException(
                        "Cannot convert column of type " + source.getType() + " to DateTime");
            }
            // noinspection unchecked
            return source.allowsReinterpret(DateTime.class) ? source.reinterpret(DateTime.class)
                    : new BoxedColumnSource.OfDateTime((ColumnSource<Long>) source);
        }
        throw new UnsupportedOperationException("Unsupported original type " + originalType);
    }
}
