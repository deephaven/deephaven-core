//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Consumer;

public class ReinterpretUtils {

    /**
     * Given a {@code byte} column source turn it into a {@link Boolean} column source, either via reinterpretation or
     * wrapping.
     *
     * @param source the source to turn into a {@link Boolean} source
     *
     * @return the {@link Boolean} source
     */
    @NotNull
    public static ColumnSource<Boolean> byteToBooleanSource(@NotNull final ColumnSource<Byte> source) {
        if (source.allowsReinterpret(Boolean.class)) {
            return source.reinterpret(Boolean.class);
        } else {
            return new ByteAsBooleanColumnSource(source);
        }
    }

    /**
     * Given a {@link Boolean} column source turn it into a {@code byte} column source, either via reinterpretation or
     * wrapping.
     *
     * @param source the source to turn into a {@code byte} source
     *
     * @return the {@code byte} source
     */
    @NotNull
    public static ColumnSource<Byte> booleanToByteSource(@NotNull final ColumnSource<Boolean> source) {
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
    @Nullable
    public static WritableColumnSource<Byte> writableBooleanToByteSource(
            @NotNull final WritableColumnSource<Boolean> source) {
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
    @NotNull
    public static ColumnSource<Instant> longToInstantSource(@NotNull final ColumnSource<Long> source) {
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
    public static ColumnSource<Long> instantToLongSource(@NotNull final ColumnSource<Instant> source) {
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
    @Nullable
    public static WritableColumnSource<Long> writableInstantToLongSource(
            @NotNull final WritableColumnSource<Instant> source) {
        if (source.allowsReinterpret(long.class)) {
            return (WritableColumnSource<Long>) source.reinterpret(long.class);
        }
        return null;
    }

    /**
     * Given a {@code long} column source turn it into a {@link ZonedDateTime} column source, either via
     * reinterpretation or wrapping.
     *
     * @param source the source to turn into a {@link ZonedDateTime} source
     * @param wrapperTimeZone the {@link ZoneId} to use if and only if we can't apply a simple reinterpret
     *
     * @return the {@code long} source
     */
    public static ColumnSource<ZonedDateTime> longToZonedDateTimeSource(
            @NotNull final ColumnSource<Long> source,
            @NotNull final ZoneId wrapperTimeZone) {
        if (source.allowsReinterpret(ZonedDateTime.class)) {
            return source.reinterpret(ZonedDateTime.class);
        } else {
            return new LongAsZonedDateTimeColumnSource(source, wrapperTimeZone);
        }
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
    public static ColumnSource<Long> zonedDateTimeToLongSource(@NotNull final ColumnSource<ZonedDateTime> source) {
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
    @Nullable
    public static WritableColumnSource<Long> writableZonedDateTimeToLongSource(
            @NotNull final WritableColumnSource<ZonedDateTime> source) {
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
    @NotNull
    public static ColumnSource<?> maybeConvertToPrimitive(@NotNull final ColumnSource<?> source) {
        if (source.getType() == Boolean.class || source.getType() == boolean.class) {
            return booleanToByteSource((ColumnSource<Boolean>) source);
        }
        if (source.getType() == Instant.class) {
            return instantToLongSource((ColumnSource<Instant>) source);
        }
        if (source.getType() == ZonedDateTime.class) {
            // We require this to be symmetrical with convertToOriginalType. This means we must restrict conversion to
            // sources where we can find the time zone when we need to convert back.
            // TODO (https://github.com/deephaven/deephaven-core/issues/3455): Do better with richer types
            if (source instanceof ConvertibleTimeSource.Zoned) {
                return zonedDateTimeToLongSource((ColumnSource<ZonedDateTime>) source);
            }
        }
        return source;
    }

    /**
     * Convert each source in {@code sources} to a primitive if possible.
     *
     * @param sources An array of the sources to potentially convert
     * @return The primitive sources for each source in {@code sources}
     */
    @NotNull
    public static ColumnSource<?>[] maybeConvertToPrimitive(@NotNull final ColumnSource<?>[] sources) {
        final ColumnSource<?>[] result = new ColumnSource<?>[sources.length];
        for (int ii = 0; ii < sources.length; ++ii) {
            result[ii] = maybeConvertToPrimitive(sources[ii]);
        }
        return result;
    }

    /**
     * If {@code source} is something that we prefer to handle as a primitive, do the appropriate conversion.
     *
     * @param source the source to convert
     * @return if possible, {@code source} converted to a writable primitive, otherwise {@code source}
     */
    @SuppressWarnings("unchecked")
    @NotNull
    public static WritableColumnSource<?> maybeConvertToWritablePrimitive(
            @NotNull final WritableColumnSource<?> source) {
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
    @NotNull
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
     * If {@code dataType} is something that we prefer to handle as a primitive, emit the appropriate {@link ChunkType},
     * else the normal ChunkType for the data type.
     *
     * @param dataType The data type to convert to a {@link ChunkType}
     * @return the appropriate {@link ChunkType} to use when writing primitives to the destination
     */
    @NotNull
    public static ChunkType maybeConvertToWritablePrimitiveChunkType(@NotNull final Class<?> dataType) {
        if (dataType == Boolean.class || dataType == boolean.class) {
            return ChunkType.Byte;
        }
        if (dataType == Instant.class) {
            // Note that storing ZonedDateTime as a primitive is lossy on the time zone.
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
    @NotNull
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
     * Reinterpret or box {@code sourceToConvert} back to its original type.
     *
     * @param originalSource The source that was reinterpreted to produce {@code sourceToConvert}, or a similarly-typed
     *        source for type information
     * @param sourceToConvert The source to convert
     * @return reinterpret or box {@code sourceToConvert} back to the original type if possible
     * @throws UnsupportedOperationException for unsupported conversions
     */
    @NotNull
    public static ColumnSource<?> convertToOriginalType(
            @NotNull final ColumnSource<?> originalSource,
            @NotNull final ColumnSource<?> sourceToConvert) {

        final Class<?> originalType = originalSource.getType();
        final Consumer<Class<?>> validateSourceType = expectedType -> {
            if (sourceToConvert.getType() != expectedType) {
                throw new UnsupportedOperationException(String.format(
                        "Cannot convert column of type %s to %s", sourceToConvert.getType(), originalType));
            }
        };

        if (originalType == Boolean.class) {
            validateSourceType.accept(byte.class);
            // noinspection unchecked
            return byteToBooleanSource((ColumnSource<Byte>) sourceToConvert);
        }

        if (originalType == Instant.class) {
            validateSourceType.accept(long.class);
            // noinspection unchecked
            return longToInstantSource((ColumnSource<Long>) sourceToConvert);
        }

        if (originalType == ZonedDateTime.class) {
            validateSourceType.accept(long.class);
            if (originalSource instanceof ConvertibleTimeSource.Zoned) {
                final ConvertibleTimeSource.Zoned zonedOriginal = (ConvertibleTimeSource.Zoned) originalSource;
                // noinspection unchecked
                return longToZonedDateTimeSource((ColumnSource<Long>) sourceToConvert, zonedOriginal.getZone());
            }
            throw new UnsupportedOperationException(String.format(
                    "Unsupported original source class %s for converting long to ZonedDateTime",
                    originalSource.getClass()));
        }

        throw new UnsupportedOperationException((String.format("Unsupported original type %s", originalType)));
    }
}
