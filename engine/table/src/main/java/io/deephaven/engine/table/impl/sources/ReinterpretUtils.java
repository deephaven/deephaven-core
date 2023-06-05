/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.function.Consumer;

public class ReinterpretUtils {

    /**
     * Given a {@link Boolean} column source turn it into a {@code byte} column source, either via reinterpretation or
     * wrapping.
     *
     * @param source the source to turn into a {@code byte} source
     *
     * @return the {@code byte} source
     */
    public static ColumnSource<Byte> booleanToByteSource(ColumnSource<Boolean> source) {
        if (source.allowsReinterpret(byte.class)) {
            return source.reinterpret(byte.class);
        } else {
            return new BooleanAsByteColumnSource(source);
        }
    }

    /**
     * Given a writable {@link Boolean} column source turn it into a writable {@code byte} column source via
     * reinterpretation if possible.
     *
     * @param source the source to turn into a {@code byte} source
     *
     * @return the {@code byte} source or null if it could not be reinterpreted
     */
    public static WritableColumnSource<Byte> writableBooleanToByteSource(WritableColumnSource<Boolean> source) {
        if (source.allowsReinterpret(byte.class)) {
            return (WritableColumnSource<Byte>) source.reinterpret(byte.class);
        }
        return null;
    }

    /**
     * Given a {@code long} column source turn it into an {@link Instant} column source, either via reinterpretation or
     * wrapping.
     *
     * @param source the source to turn into an {@link Instant} source
     *
     * @return the {@code long} source
     */
    public static ColumnSource<Instant> longToInstantSource(ColumnSource<Long> source) {
        if (source.allowsReinterpret(Instant.class)) {
            return source.reinterpret(Instant.class);
        } else {
            return new LongAsInstantColumnSource(source);
        }
    }

    /**
     * Given an {@link Instant} column source turn it into a {@code long} column source, either via reinterpretation or
     * wrapping.
     *
     * @param source the source to turn into a {@code long} source
     *
     * @return the {@code long} source
     */
    @NotNull
    public static ColumnSource<Long> instantToLongSource(final @NotNull ColumnSource<Instant> source) {
        if (source.allowsReinterpret(long.class)) {
            return source.reinterpret(long.class);
        } else {
            return new InstantAsLongColumnSource(source);
        }
    }

    /**
     * Given a writable {@link Instant} column source turn it into a writable {@code long} column source via
     * reinterpretation if possible.
     *
     * @param source the source to turn into a {@code long} source
     *
     * @return the {@code long} source or null if it could not be reinterpreted
     */
    public static WritableColumnSource<Long> writableInstantToLongSource(
            final @NotNull WritableColumnSource<Instant> source) {
        if (source.allowsReinterpret(long.class)) {
            return (WritableColumnSource<Long>) source.reinterpret(long.class);
        }
        return null;
    }

    /**
     * Given a {@link ZonedDateTime} column source turn it into a {@code long} column source, either via
     * reinterpretation or wrapping.
     *
     * @param source the source to turn into a {@code long} source
     *
     * @return the {@code long} source
     */
    @NotNull
    public static ColumnSource<Long> zonedDateTimeToLongSource(final @NotNull ColumnSource<ZonedDateTime> source) {
        if (source.allowsReinterpret(long.class)) {
            return source.reinterpret(long.class);
        } else {
            return new ZonedDateTimeAsLongSource(source);
        }
    }

    /**
     * Given a writable {@link ZonedDateTime} column source turn it into a writable {@code long} column source via
     * reinterpretation if possible.
     *
     * @param source the source to turn into a {@code long} source
     *
     * @return the {@code long} source or null if it could not be reinterpreted
     */
    public static WritableColumnSource<Long> writableZonedDateTimeToLongSource(
            final @NotNull WritableColumnSource<ZonedDateTime> source) {
        if (source.allowsReinterpret(long.class)) {
            return (WritableColumnSource<Long>) source.reinterpret(long.class);
        }
        return null;
    }

    /**
     * If source is something that we prefer to handle as a primitive, do the appropriate conversion.
     *
     * @param source The source to convert
     * @return if possible, the source converted to a primitive, otherwise the source
     */
    @SuppressWarnings("unchecked")
    public static ColumnSource<?> maybeConvertToPrimitive(ColumnSource<?> source) {
        if (source.getType() == Boolean.class || source.getType() == boolean.class) {
            return booleanToByteSource((ColumnSource<Boolean>) source);
        }
        if (source.getType() == Instant.class) {
            return instantToLongSource((ColumnSource<Instant>) source);
        }
        if (source.getType() == ZonedDateTime.class) {
            return zonedDateTimeToLongSource((ColumnSource<ZonedDateTime>) source);
        }
        return source;
    }


    /**
     * If source is something that we prefer to handle as a primitive, do the appropriate conversion.
     *
     * @param source The source to convert
     * @return if possible, the source converted to a writable primitive, otherwise the source
     */
    @SuppressWarnings("unchecked")
    public static WritableColumnSource<?> maybeConvertToWritablePrimitive(WritableColumnSource<?> source) {
        WritableColumnSource<?> result = null;
        if (source.getType() == Boolean.class || source.getType() == boolean.class) {
            result = writableBooleanToByteSource((WritableColumnSource<Boolean>) source);
        } else if (source.getType() == Instant.class) {
            result = writableInstantToLongSource((WritableColumnSource<Instant>) source);
        } else if (source.getType() == ZonedDateTime.class) {
            result = writableZonedDateTimeToLongSource((WritableColumnSource<ZonedDateTime>) source);
        }
        return result == null ? source : result;
    }

    /**
     * If {@code dataType} is something that we prefer to handle as a primitive, emit the appropriate {@link ChunkType},
     * else the normal ChunkType for the data type.
     *
     * @param dataType The data type to convert to a {@link ChunkType}
     * @return the appropriate {@link ChunkType} to use when extracting primitives from the source
     */
    public static ChunkType maybeConvertToPrimitiveChunkType(@NotNull final Class<?> dataType) {
        if (dataType == Boolean.class || dataType == boolean.class) {
            return ChunkType.Byte;
        }
        if (dataType == Instant.class || dataType == ZonedDateTime.class) {
            return ChunkType.Long;
        }
        return ChunkType.fromElementType(dataType);
    }

    /**
     * If {@code dataType} is something that we prefer to handle as a primitive, emit the appropriate {@link Class data
     * type to use}, else return {@code dataType}.
     *
     * @param dataType The data type to examine
     * @return the appropriate data type to use when extracting primitives from the source
     */
    public static Class<?> maybeConvertToPrimitiveDataType(@NotNull final Class<?> dataType) {
        if (dataType == Boolean.class || dataType == boolean.class) {
            return byte.class;
        }
        if (dataType == Instant.class || dataType == ZonedDateTime.class) {
            return long.class;
        }
        return dataType;
    }

    /**
     * Reinterpret or box {@link ColumnSource} back to its original type.
     *
     * @param originalType The type to convert to
     * @param source The source to convert
     * @return reinterpret or box source back to the original type if possible
     */
    public static ColumnSource<?> convertToOriginal(
            @NotNull final Class<?> originalType,
            @NotNull final ColumnSource<?> source) {

        final Consumer<Class<?>> validateSourceType = expectedType -> {
            if (source.getType() != expectedType) {
                throw new UnsupportedOperationException(
                        "Cannot convert column of type " + source.getType() + " to " + originalType);
            }
        };

        if (originalType == Boolean.class) {
            validateSourceType.accept(byte.class);
            // noinspection unchecked
            return source.allowsReinterpret(Boolean.class) ? source.reinterpret(Boolean.class)
                    : new BoxedColumnSource.OfBoolean((ColumnSource<Byte>) source);
        }
        if (originalType == Instant.class) {
            validateSourceType.accept(long.class);
            // noinspection unchecked
            return source.allowsReinterpret(Instant.class) ? source.reinterpret(Instant.class)
                    : new BoxedColumnSource.OfInstant((ColumnSource<Long>) source);
        }
        throw new UnsupportedOperationException("Unsupported original type " + originalType);
    }
}
