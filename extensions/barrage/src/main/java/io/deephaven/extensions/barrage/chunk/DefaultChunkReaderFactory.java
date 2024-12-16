//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.base.Charsets;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.extensions.barrage.chunk.array.ArrayExpansionKernel;
import io.deephaven.extensions.barrage.chunk.vector.VectorExpansionKernel;
import io.deephaven.extensions.barrage.util.ArrowIpcUtil;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * JVM implementation of {@link ChunkReader.Factory}, suitable for use in Java clients and servers. This default
 * implementation may not round trip flight types in a stable way, but will round trip Deephaven table definitions and
 * table data. Neither of these is a required/expected property of being a Flight/Barrage/Deephaven client.
 */
public class DefaultChunkReaderFactory implements ChunkReader.Factory {
    static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
    static final Set<ArrowType.ArrowTypeID> SPECIAL_TYPES = Set.of(
            ArrowType.ArrowTypeID.List,
            ArrowType.ArrowTypeID.FixedSizeList,
            ArrowType.ArrowTypeID.Map,
            ArrowType.ArrowTypeID.Struct,
            ArrowType.ArrowTypeID.Union,
            ArrowType.ArrowTypeID.Null);

    public static final Logger log = LoggerFactory.getLogger(DefaultChunkReaderFactory.class);
    public static final ChunkReader.Factory INSTANCE = new DefaultChunkReaderFactory();

    protected interface ChunkReaderFactory {
        ChunkReader<? extends WritableChunk<Values>> make(
                final ArrowType arrowType,
                final BarrageTypeInfo<Field> typeInfo,
                final BarrageOptions options);
    }

    // allow subclasses to modify this as they wish
    protected final Map<ArrowType.ArrowTypeID, Map<Class<?>, ChunkReaderFactory>> registeredFactories =
            new HashMap<>();

    protected DefaultChunkReaderFactory() {
        register(ArrowType.ArrowTypeID.Timestamp, long.class, DefaultChunkReaderFactory::timestampToLong);
        register(ArrowType.ArrowTypeID.Timestamp, Instant.class, DefaultChunkReaderFactory::timestampToInstant);
        register(ArrowType.ArrowTypeID.Timestamp, ZonedDateTime.class,
                DefaultChunkReaderFactory::timestampToZonedDateTime);
        register(ArrowType.ArrowTypeID.Timestamp, LocalDateTime.class,
                DefaultChunkReaderFactory::timestampToLocalDateTime);
        register(ArrowType.ArrowTypeID.Utf8, String.class, DefaultChunkReaderFactory::utf8ToString);
        register(ArrowType.ArrowTypeID.Duration, long.class, DefaultChunkReaderFactory::durationToLong);
        register(ArrowType.ArrowTypeID.Duration, Duration.class, DefaultChunkReaderFactory::durationToDuration);
        register(ArrowType.ArrowTypeID.FloatingPoint, float.class, DefaultChunkReaderFactory::floatingPointToFloat);
        register(ArrowType.ArrowTypeID.FloatingPoint, double.class, DefaultChunkReaderFactory::floatingPointToDouble);
        register(ArrowType.ArrowTypeID.FloatingPoint, BigDecimal.class,
                DefaultChunkReaderFactory::floatingPointToBigDecimal);
        register(ArrowType.ArrowTypeID.Binary, byte[].class, DefaultChunkReaderFactory::binaryToByteArray);
        register(ArrowType.ArrowTypeID.Binary, String.class, DefaultChunkReaderFactory::utf8ToString);
        register(ArrowType.ArrowTypeID.Binary, BigInteger.class, DefaultChunkReaderFactory::binaryToBigInt);
        register(ArrowType.ArrowTypeID.Binary, BigDecimal.class, DefaultChunkReaderFactory::binaryToBigDecimal);
        register(ArrowType.ArrowTypeID.Binary, Schema.class, DefaultChunkReaderFactory::binaryToSchema);
        register(ArrowType.ArrowTypeID.Time, long.class, DefaultChunkReaderFactory::timeToLong);
        register(ArrowType.ArrowTypeID.Time, LocalTime.class, DefaultChunkReaderFactory::timeToLocalTime);
        register(ArrowType.ArrowTypeID.Decimal, byte.class, DefaultChunkReaderFactory::decimalToByte);
        register(ArrowType.ArrowTypeID.Decimal, char.class, DefaultChunkReaderFactory::decimalToChar);
        register(ArrowType.ArrowTypeID.Decimal, short.class, DefaultChunkReaderFactory::decimalToShort);
        register(ArrowType.ArrowTypeID.Decimal, int.class, DefaultChunkReaderFactory::decimalToInt);
        register(ArrowType.ArrowTypeID.Decimal, long.class, DefaultChunkReaderFactory::decimalToLong);
        register(ArrowType.ArrowTypeID.Decimal, BigInteger.class, DefaultChunkReaderFactory::decimalToBigInteger);
        register(ArrowType.ArrowTypeID.Decimal, float.class, DefaultChunkReaderFactory::decimalToFloat);
        register(ArrowType.ArrowTypeID.Decimal, double.class, DefaultChunkReaderFactory::decimalToDouble);
        register(ArrowType.ArrowTypeID.Decimal, BigDecimal.class, DefaultChunkReaderFactory::decimalToBigDecimal);
        register(ArrowType.ArrowTypeID.Int, byte.class, DefaultChunkReaderFactory::intToByte);
        register(ArrowType.ArrowTypeID.Int, char.class, DefaultChunkReaderFactory::intToChar);
        register(ArrowType.ArrowTypeID.Int, short.class, DefaultChunkReaderFactory::intToShort);
        register(ArrowType.ArrowTypeID.Int, int.class, DefaultChunkReaderFactory::intToInt);
        register(ArrowType.ArrowTypeID.Int, long.class, DefaultChunkReaderFactory::intToLong);
        register(ArrowType.ArrowTypeID.Int, BigInteger.class, DefaultChunkReaderFactory::intToBigInt);
        register(ArrowType.ArrowTypeID.Int, float.class, DefaultChunkReaderFactory::intToFloat);
        register(ArrowType.ArrowTypeID.Int, double.class, DefaultChunkReaderFactory::intToDouble);
        register(ArrowType.ArrowTypeID.Int, BigDecimal.class, DefaultChunkReaderFactory::intToBigDecimal);
        register(ArrowType.ArrowTypeID.Bool, boolean.class, DefaultChunkReaderFactory::boolToBoolean);
        register(ArrowType.ArrowTypeID.Bool, Boolean.class, DefaultChunkReaderFactory::boolToBoolean);
        // note that we hold boolean's in ByteChunks, so it's identical logic to read boolean as bytes.
        register(ArrowType.ArrowTypeID.Bool, byte.class, DefaultChunkReaderFactory::boolToBoolean);
        register(ArrowType.ArrowTypeID.FixedSizeBinary, byte[].class,
                DefaultChunkReaderFactory::fixedSizeBinaryToByteArray);
        register(ArrowType.ArrowTypeID.Date, int.class, DefaultChunkReaderFactory::dateToInt);
        register(ArrowType.ArrowTypeID.Date, long.class, DefaultChunkReaderFactory::dateToLong);
        register(ArrowType.ArrowTypeID.Date, LocalDate.class, DefaultChunkReaderFactory::dateToLocalDate);
        register(ArrowType.ArrowTypeID.Interval, long.class, DefaultChunkReaderFactory::intervalToDurationLong);
        register(ArrowType.ArrowTypeID.Interval, Duration.class, DefaultChunkReaderFactory::intervalToDuration);
        register(ArrowType.ArrowTypeID.Interval, Period.class, DefaultChunkReaderFactory::intervalToPeriod);
        register(ArrowType.ArrowTypeID.Interval, PeriodDuration.class,
                DefaultChunkReaderFactory::intervalToPeriodDuration);
    }

    @Override
    public <T extends WritableChunk<Values>> ChunkReader<T> newReader(
            @NotNull final BarrageTypeInfo<org.apache.arrow.flatbuf.Field> typeInfo,
            @NotNull final BarrageOptions options) {
        final BarrageTypeInfo<Field> fieldTypeInfo = new BarrageTypeInfo<>(
                typeInfo.type(),
                typeInfo.componentType(),
                Field.convertField(typeInfo.arrowField()));
        return newReaderPojo(fieldTypeInfo, options, true);
    }

    public <T extends WritableChunk<Values>> ChunkReader<T> newReaderPojo(
            @NotNull final BarrageTypeInfo<Field> typeInfo,
            @NotNull final BarrageOptions options,
            final boolean isTopLevel) {
        // TODO (deephaven/deephaven-core#6033): Run-End Support
        // TODO (deephaven/deephaven-core#6034): Dictionary Support

        final Field field = typeInfo.arrowField();

        final ArrowType.ArrowTypeID typeId = field.getType().getTypeID();
        final boolean isSpecialType = SPECIAL_TYPES.contains(typeId);

        // TODO (deephaven/deephaven-core#6038): these arrow types require 64-bit offsets
        if (typeId == ArrowType.ArrowTypeID.LargeUtf8
                || typeId == ArrowType.ArrowTypeID.LargeBinary
                || typeId == ArrowType.ArrowTypeID.LargeList
                || typeId == ArrowType.ArrowTypeID.LargeListView) {
            throw new UnsupportedOperationException(String.format(
                    "No support for 64-bit offsets to map arrow type %s to %s.",
                    field.getType().toString(),
                    typeInfo.type().getCanonicalName()));
        }

        final Map<Class<?>, ChunkReaderFactory> knownReaders = registeredFactories.get(typeId);
        if (knownReaders == null && !isSpecialType) {
            throw new UnsupportedOperationException(String.format(
                    "No known ChunkReader for arrow type %s to %s.",
                    field.getType().toString(),
                    typeInfo.type().getCanonicalName()));
        }

        final ChunkReaderFactory chunkReaderFactory = knownReaders == null ? null : knownReaders.get(typeInfo.type());
        if (chunkReaderFactory != null) {
            // noinspection unchecked
            final ChunkReader<T> reader = (ChunkReader<T>) chunkReaderFactory.make(field.getType(), typeInfo, options);
            if (reader != null) {
                return reader;
            }
        } else if (!isSpecialType) {
            throw new UnsupportedOperationException(String.format(
                    "No known ChunkReader for arrow type %s to %s. Supported types: %s",
                    field.getType().toString(),
                    typeInfo.type().getCanonicalName(),
                    knownReaders.keySet().stream().map(Object::toString).collect(Collectors.joining(", "))));
        }

        if (typeId == ArrowType.ArrowTypeID.Null) {
            return new NullChunkReader<>(typeInfo.type());
        }

        if (typeId == ArrowType.ArrowTypeID.List
                || typeId == ArrowType.ArrowTypeID.ListView
                || typeId == ArrowType.ArrowTypeID.FixedSizeList) {

            int fixedSizeLength = 0;
            final ListChunkReader.Mode mode;
            if (typeId == ArrowType.ArrowTypeID.List) {
                mode = ListChunkReader.Mode.VARIABLE;
            } else if (typeId == ArrowType.ArrowTypeID.ListView) {
                mode = ListChunkReader.Mode.VIEW;
            } else {
                mode = ListChunkReader.Mode.FIXED;
                fixedSizeLength = ((ArrowType.FixedSizeList) field.getType()).getListSize();
            }

            final BarrageTypeInfo<Field> componentTypeInfo;
            final boolean useVectorKernels = Vector.class.isAssignableFrom(typeInfo.type());
            if (useVectorKernels) {
                final Class<?> componentType =
                        VectorExpansionKernel.getComponentType(typeInfo.type(), typeInfo.componentType());
                componentTypeInfo = new BarrageTypeInfo<>(
                        componentType,
                        componentType.getComponentType(),
                        typeInfo.arrowField().getChildren().get(0));
            } else if (typeInfo.type().isArray()) {
                final Class<?> componentType = typeInfo.componentType();
                // noinspection DataFlowIssue
                componentTypeInfo = new BarrageTypeInfo<>(
                        componentType,
                        componentType.getComponentType(),
                        typeInfo.arrowField().getChildren().get(0));
            } else if (isTopLevel && options.columnsAsList()) {
                final BarrageTypeInfo<Field> realTypeInfo = new BarrageTypeInfo<>(
                        typeInfo.type(),
                        typeInfo.componentType(),
                        typeInfo.arrowField().getChildren().get(0));
                final ChunkReader<WritableChunk<Values>> componentReader = newReaderPojo(realTypeInfo, options, false);
                // noinspection unchecked
                return (ChunkReader<T>) new SingleElementListHeaderReader<>(componentReader);
            } else {
                throw new UnsupportedOperationException(String.format(
                        "No known ChunkReader for arrow type %s to %s. Expected destination type to be an array.",
                        field.getType().toString(),
                        typeInfo.type().getCanonicalName()));
            }

            final ChunkType chunkType = ListChunkReader.getChunkTypeFor(componentTypeInfo.type());
            final ExpansionKernel<?> kernel;
            if (useVectorKernels) {
                kernel = VectorExpansionKernel.makeExpansionKernel(chunkType, componentTypeInfo.type());
            } else {
                kernel = ArrayExpansionKernel.makeExpansionKernel(chunkType, componentTypeInfo.type());
            }
            final ChunkReader<WritableChunk<Values>> componentReader = newReaderPojo(componentTypeInfo, options, false);

            // noinspection unchecked
            return (ChunkReader<T>) new ListChunkReader<>(mode, fixedSizeLength, kernel, componentReader);
        }

        if (typeId == ArrowType.ArrowTypeID.Map) {
            final Field structField = field.getChildren().get(0);
            final BarrageTypeInfo<Field> keyTypeInfo = BarrageUtil.getDefaultType(structField.getChildren().get(0));
            final BarrageTypeInfo<Field> valueTypeInfo = BarrageUtil.getDefaultType(structField.getChildren().get(1));

            final ChunkReader<WritableChunk<Values>> keyReader = newReaderPojo(keyTypeInfo, options, false);
            final ChunkReader<WritableChunk<Values>> valueReader = newReaderPojo(valueTypeInfo, options, false);

            // noinspection unchecked
            return (ChunkReader<T>) new MapChunkReader<>(keyReader, valueReader);
        }

        // TODO: if (typeId == ArrowType.ArrowTypeID.Struct) {
        // expose transformer API of Map<String, Chunk<Values>> -> T
        // maybe defaults to Map<String, Object>

        if (typeId == ArrowType.ArrowTypeID.Union) {
            final ArrowType.Union unionType = (ArrowType.Union) field.getType();
            final List<ChunkReader<? extends WritableChunk<Values>>> innerReaders = new ArrayList<>();

            for (int ii = 0; ii < field.getChildren().size(); ++ii) {
                final Field childField = field.getChildren().get(ii);
                final BarrageTypeInfo<Field> childTypeInfo = BarrageUtil.getDefaultType(childField);
                ChunkReader<? extends WritableChunk<Values>> childReader = newReaderPojo(childTypeInfo, options, false);
                if (childField.getType().getTypeID() == ArrowType.ArrowTypeID.Bool) {
                    childReader = ((BooleanChunkReader) childReader).transform(BooleanUtils::byteAsBoolean);
                }
                innerReaders.add(childReader);
            }

            // noinspection unchecked
            return (ChunkReader<T>) new UnionChunkReader<T>(
                    UnionChunkReader.mode(unionType.getMode()), innerReaders);
        }

        throw new UnsupportedOperationException(String.format(
                "No known ChunkReader for arrow type %s to %s. Arrow type supports: %s",
                field.getType().toString(),
                typeInfo.type().getCanonicalName(),
                knownReaders == null ? "none"
                        : knownReaders.keySet().stream()
                                .map(Object::toString)
                                .collect(Collectors.joining(", "))));
    }

    @SuppressWarnings("unchecked")
    protected void register(
            final ArrowType.ArrowTypeID arrowType,
            final Class<?> deephavenType,
            final ChunkReaderFactory chunkReaderFactory) {
        registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                .put(deephavenType, chunkReaderFactory);

        // if primitive automatically register the boxed version of this mapping, too
        if (deephavenType == byte.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Byte.class, (at, typeInfo, options) -> transformToObject(
                            (ChunkReader<WritableByteChunk<Values>>) chunkReaderFactory.make(at, typeInfo, options),
                            (chunk, ii) -> TypeUtils.box(chunk.get(ii))));
        } else if (deephavenType == short.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Short.class, (at, typeInfo, options) -> transformToObject(
                            (ChunkReader<WritableShortChunk<Values>>) chunkReaderFactory.make(at, typeInfo, options),
                            (chunk, ii) -> TypeUtils.box(chunk.get(ii))));
        } else if (deephavenType == int.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Integer.class, (at, typeInfo, options) -> transformToObject(
                            (ChunkReader<WritableIntChunk<Values>>) chunkReaderFactory.make(at, typeInfo, options),
                            (chunk, ii) -> TypeUtils.box(chunk.get(ii))));
        } else if (deephavenType == long.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Long.class, (at, typeInfo, options) -> transformToObject(
                            (ChunkReader<WritableLongChunk<Values>>) chunkReaderFactory.make(at, typeInfo, options),
                            (chunk, ii) -> TypeUtils.box(chunk.get(ii))));
        } else if (deephavenType == char.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Character.class, (at, typeInfo, options) -> transformToObject(
                            (ChunkReader<WritableCharChunk<Values>>) chunkReaderFactory.make(at, typeInfo, options),
                            (chunk, ii) -> TypeUtils.box(chunk.get(ii))));
        } else if (deephavenType == float.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Float.class, (at, typeInfo, options) -> transformToObject(
                            (ChunkReader<WritableFloatChunk<Values>>) chunkReaderFactory.make(at, typeInfo, options),
                            (chunk, ii) -> TypeUtils.box(chunk.get(ii))));
        } else if (deephavenType == double.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Double.class, (at, typeInfo, options) -> transformToObject(
                            (ChunkReader<WritableDoubleChunk<Values>>) chunkReaderFactory.make(at, typeInfo, options),
                            (chunk, ii) -> TypeUtils.box(chunk.get(ii))));
        }
    }

    private static long factorForTimeUnit(final TimeUnit unit) {
        switch (unit) {
            case NANOSECOND:
                return 1;
            case MICROSECOND:
                return 1000;
            case MILLISECOND:
                return 1000 * 1000L;
            case SECOND:
                return 1000 * 1000 * 1000L;
            default:
                throw new IllegalArgumentException("Unexpected time unit value: " + unit);
        }
    }

    private static ChunkReader<WritableLongChunk<Values>> timestampToLong(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final long factor = factorForTimeUnit(((ArrowType.Timestamp) arrowType).getUnit());
        return factor == 1
                ? new LongChunkReader(options)
                : new LongChunkReader(options,
                        (long v) -> v == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : (v * factor));
    }

    private static ChunkReader<WritableObjectChunk<Instant, Values>> timestampToInstant(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final long factor = factorForTimeUnit(((ArrowType.Timestamp) arrowType).getUnit());
        return new FixedWidthChunkReader<>(Long.BYTES, true, options, io -> {
            final long value = io.readLong();
            if (value == QueryConstants.NULL_LONG) {
                return null;
            }
            return DateTimeUtils.epochNanosToInstant(value * factor);
        });
    }

    private static ChunkReader<WritableObjectChunk<ZonedDateTime, Values>> timestampToZonedDateTime(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowType;
        final String timezone = tsType.getTimezone();
        final ZoneId tz = timezone == null ? ZoneId.systemDefault() : DateTimeUtils.parseTimeZone(timezone);
        final long factor = factorForTimeUnit(tsType.getUnit());
        return new FixedWidthChunkReader<>(Long.BYTES, true, options, io -> {
            final long value = io.readLong();
            if (value == QueryConstants.NULL_LONG) {
                return null;
            }
            return DateTimeUtils.epochNanosToZonedDateTime(value * factor, tz);
        });
    }

    private static ChunkReader<WritableObjectChunk<LocalDateTime, Values>> timestampToLocalDateTime(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowType;
        final ZoneId tz = DateTimeUtils.parseTimeZone(tsType.getTimezone());
        final long factor = factorForTimeUnit(tsType.getUnit());
        return new FixedWidthChunkReader<>(Long.BYTES, true, options, io -> {
            final long value = io.readLong();
            if (value == QueryConstants.NULL_LONG) {
                return null;
            }
            // noinspection DataFlowIssue
            return DateTimeUtils.epochNanosToZonedDateTime(value * factor, tz).toLocalDateTime();
        });
    }

    private static ChunkReader<WritableObjectChunk<String, Values>> utf8ToString(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return new VarBinaryChunkReader<>((buf, off, len) -> new String(buf, off, len, Charsets.UTF_8));
    }

    private static ChunkReader<WritableLongChunk<Values>> durationToLong(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final long factor = factorForTimeUnit(((ArrowType.Duration) arrowType).getUnit());
        return factor == 1
                ? new LongChunkReader(options)
                : new LongChunkReader(options,
                        (long v) -> v == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : (v * factor));
    }

    private static ChunkReader<WritableObjectChunk<Duration, Values>> durationToDuration(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final long factor = factorForTimeUnit(((ArrowType.Duration) arrowType).getUnit());
        return transformToObject(new LongChunkReader(options), (chunk, ii) -> {
            long value = chunk.get(ii);
            return value == QueryConstants.NULL_LONG ? null : Duration.ofNanos(value * factor);
        });
    }

    private static ChunkReader<WritableFloatChunk<Values>> floatingPointToFloat(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return new FloatChunkReader(((ArrowType.FloatingPoint) arrowType).getPrecision().getFlatbufID(), options);
    }

    private static ChunkReader<WritableDoubleChunk<Values>> floatingPointToDouble(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return new DoubleChunkReader(((ArrowType.FloatingPoint) arrowType).getPrecision().getFlatbufID(), options);
    }

    private static ChunkReader<WritableObjectChunk<BigDecimal, Values>> floatingPointToBigDecimal(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return transformToObject(
                new DoubleChunkReader(((ArrowType.FloatingPoint) arrowType).getPrecision().getFlatbufID(), options),
                (chunk, ii) -> {
                    double value = chunk.get(ii);
                    return value == QueryConstants.NULL_DOUBLE ? null : BigDecimal.valueOf(value);
                });
    }

    private static ChunkReader<WritableObjectChunk<byte[], Values>> binaryToByteArray(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return new VarBinaryChunkReader<>((buf, off, len) -> Arrays.copyOfRange(buf, off, off + len));
    }

    private static ChunkReader<WritableObjectChunk<BigInteger, Values>> binaryToBigInt(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return new VarBinaryChunkReader<>(BigInteger::new);
    }

    private static ChunkReader<WritableObjectChunk<BigDecimal, Values>> binaryToBigDecimal(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return new VarBinaryChunkReader<>((final byte[] buf, final int offset, final int length) -> {
            // read the int scale value as little endian, arrow's endianness.
            final byte b1 = buf[offset];
            final byte b2 = buf[offset + 1];
            final byte b3 = buf[offset + 2];
            final byte b4 = buf[offset + 3];
            final int scale = b4 << 24 | (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);
            return new BigDecimal(new BigInteger(buf, offset + 4, length - 4), scale);
        });
    }

    private static ChunkReader<WritableObjectChunk<Schema, Values>> binaryToSchema(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return new VarBinaryChunkReader<>(ArrowIpcUtil::deserialize);
    }

    private static ChunkReader<WritableLongChunk<Values>> timeToLong(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        // See timeToLocalTime's comment for more information on wire format.
        final ArrowType.Time timeType = (ArrowType.Time) arrowType;
        final int bitWidth = timeType.getBitWidth();
        final long factor = factorForTimeUnit(timeType.getUnit());
        switch (bitWidth) {
            case 32:
                return LongChunkReader.transformTo(new IntChunkReader(options), (chunk, ii) -> {
                    long value = QueryLanguageFunctionUtils.longCast(chunk.get(ii));
                    return value == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : value * factor;
                });

            case 64:
                return LongChunkReader.transformTo(new LongChunkReader(options), (chunk, ii) -> {
                    long value = chunk.get(ii);
                    return value == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : value * factor;
                });

            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkReader<WritableObjectChunk<LocalTime, Values>> timeToLocalTime(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        /*
         * Time is either a 32-bit or 64-bit signed integer type representing an elapsed time since midnight, stored in
         * either of four units: seconds, milliseconds, microseconds or nanoseconds.
         *
         * The integer `bitWidth` depends on the `unit` and must be one of the following:
         * @formatter:off
         * - SECOND and MILLISECOND: 32 bits
         * - MICROSECOND and NANOSECOND: 64 bits
         * @formatter:on
         *
         * The allowed values are between 0 (inclusive) and 86400 (=24*60*60) seconds (exclusive), adjusted for the time
         * unit (for example, up to 86400000 exclusive for the MILLISECOND unit). This definition doesn't allow for leap
         * seconds. Time values from measurements with leap seconds will need to be corrected when ingesting into Arrow
         * (for example by replacing the value 86400 with 86399).
         */

        final ArrowType.Time timeType = (ArrowType.Time) arrowType;
        final int bitWidth = timeType.getBitWidth();
        final long factor = factorForTimeUnit(timeType.getUnit());
        switch (bitWidth) {
            case 32:
                return transformToObject(new IntChunkReader(options), (chunk, ii) -> {
                    int value = chunk.get(ii);
                    return value == QueryConstants.NULL_INT ? null : LocalTime.ofNanoOfDay(value * factor);
                });

            case 64:
                return transformToObject(new LongChunkReader(options), (chunk, ii) -> {
                    long value = chunk.get(ii);
                    return value == QueryConstants.NULL_LONG ? null : LocalTime.ofNanoOfDay(value * factor);
                });

            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkReader<WritableByteChunk<Values>> decimalToByte(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return ByteChunkReader.transformTo(decimalToBigDecimal(arrowType, typeInfo, options),
                (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)));
    }

    private static ChunkReader<WritableCharChunk<Values>> decimalToChar(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return CharChunkReader.transformTo(decimalToBigDecimal(arrowType, typeInfo, options),
                (chunk, ii) -> QueryLanguageFunctionUtils.charCast(chunk.get(ii)));
    }

    private static ChunkReader<WritableShortChunk<Values>> decimalToShort(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return ShortChunkReader.transformTo(decimalToBigDecimal(arrowType, typeInfo, options),
                (chunk, ii) -> QueryLanguageFunctionUtils.shortCast(chunk.get(ii)));
    }

    private static ChunkReader<WritableIntChunk<Values>> decimalToInt(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return IntChunkReader.transformTo(decimalToBigDecimal(arrowType, typeInfo, options),
                (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)));
    }

    private static ChunkReader<WritableLongChunk<Values>> decimalToLong(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return LongChunkReader.transformTo(decimalToBigDecimal(arrowType, typeInfo, options),
                (chunk, ii) -> QueryLanguageFunctionUtils.longCast(chunk.get(ii)));
    }

    private static ChunkReader<WritableObjectChunk<BigInteger, Values>> decimalToBigInteger(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        // note this mapping is particularly useful if scale == 0
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();

        return new FixedWidthChunkReader<>(byteWidth, false, options, dataInput -> {
            final byte[] value = new byte[byteWidth];
            dataInput.readFully(value);
            if (LITTLE_ENDIAN) {
                // Decimal stored as native endian, need to swap bytes to make BigDecimal if native endian is LE
                byte temp;
                int stop = byteWidth / 2;
                for (int i = 0, j; i < stop; i++) {
                    temp = value[i];
                    j = (byteWidth - 1) - i;
                    value[i] = value[j];
                    value[j] = temp;
                }
            }

            BigInteger unscaledValue = new BigInteger(value);
            if (scale == 0) {
                return unscaledValue;
            }
            return unscaledValue.divide(BigInteger.TEN.pow(scale));
        });
    }

    private static ChunkReader<WritableFloatChunk<Values>> decimalToFloat(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return FloatChunkReader.transformTo(decimalToBigDecimal(arrowType, typeInfo, options),
                (chunk, ii) -> QueryLanguageFunctionUtils.floatCast(chunk.get(ii)));
    }

    private static ChunkReader<WritableDoubleChunk<Values>> decimalToDouble(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return DoubleChunkReader.transformTo(decimalToBigDecimal(arrowType, typeInfo, options),
                (chunk, ii) -> QueryLanguageFunctionUtils.doubleCast(chunk.get(ii)));
    }

    private static ChunkReader<WritableObjectChunk<BigDecimal, Values>> decimalToBigDecimal(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();

        return new FixedWidthChunkReader<>(byteWidth, false, options, dataInput -> {
            final byte[] value = new byte[byteWidth];
            dataInput.readFully(value);
            if (LITTLE_ENDIAN) {
                // Decimal stored as native endian, need to swap bytes to make BigDecimal if native endian is LE
                byte temp;
                int stop = byteWidth / 2;
                for (int i = 0, j; i < stop; i++) {
                    temp = value[i];
                    j = (byteWidth - 1) - i;
                    value[i] = value[j];
                    value[j] = temp;
                }
            }

            BigInteger unscaledValue = new BigInteger(value);
            return new BigDecimal(unscaledValue, scale);
        });
    }

    private static ChunkReader<WritableByteChunk<Values>> intToByte(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Int intType = (ArrowType.Int) arrowType;
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                // note unsigned mappings to byte will overflow; but user has asked for this
                return new ByteChunkReader(options);
            case 16:
                if (unsigned) {
                    // note shorts may overflow; but user has asked for this
                    return ByteChunkReader.transformTo(new CharChunkReader(options),
                            (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)));
                }
                return ByteChunkReader.transformTo(new ShortChunkReader(options),
                        (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)));
            case 32:
                // note ints may overflow; but user has asked for this
                return ByteChunkReader.transformTo(new IntChunkReader(options),
                        (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)));
            case 64:
                // note longs may overflow byte; but user has asked for this
                return ByteChunkReader.transformTo(new LongChunkReader(options),
                        (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)));
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkReader<WritableShortChunk<Values>> intToShort(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Int intType = (ArrowType.Int) arrowType;
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return ShortChunkReader.transformTo(new ByteChunkReader(options),
                        (chunk, ii) -> maskIfOverflow(unsigned, QueryLanguageFunctionUtils.shortCast(chunk.get(ii))));
            case 16:
                if (unsigned) {
                    return ShortChunkReader.transformTo(new CharChunkReader(options),
                            (chunk, ii) -> QueryLanguageFunctionUtils.shortCast(chunk.get(ii)));
                }
                return new ShortChunkReader(options);
            case 32:
                // note ints may overflow; but user has asked for this
                return ShortChunkReader.transformTo(new IntChunkReader(options),
                        (chunk, ii) -> QueryLanguageFunctionUtils.shortCast(chunk.get(ii)));
            case 64:
                // note longs may overflow; but user has asked for this
                return ShortChunkReader.transformTo(new LongChunkReader(options),
                        (chunk, ii) -> QueryLanguageFunctionUtils.shortCast(chunk.get(ii)));
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkReader<WritableIntChunk<Values>> intToInt(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Int intType = (ArrowType.Int) arrowType;
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return IntChunkReader.transformTo(new ByteChunkReader(options),
                        (chunk, ii) -> maskIfOverflow(unsigned, Byte.BYTES,
                                QueryLanguageFunctionUtils.intCast(chunk.get(ii))));
            case 16:
                if (unsigned) {
                    return IntChunkReader.transformTo(new CharChunkReader(options),
                            (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)));
                }
                return IntChunkReader.transformTo(new ShortChunkReader(options), (chunk, ii) -> maskIfOverflow(unsigned,
                        Short.BYTES, QueryLanguageFunctionUtils.intCast(chunk.get(ii))));
            case 32:
                // note unsigned int may overflow int; but user has asked for this
                return new IntChunkReader(options);
            case 64:
                // note longs may overflow int; but user has asked for this
                return IntChunkReader.transformTo(new LongChunkReader(options),
                        (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)));
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkReader<WritableLongChunk<Values>> intToLong(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Int intType = (ArrowType.Int) arrowType;
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return LongChunkReader.transformTo(new ByteChunkReader(options),
                        (chunk, ii) -> maskIfOverflow(unsigned, Byte.BYTES,
                                QueryLanguageFunctionUtils.longCast(chunk.get(ii))));
            case 16:
                if (unsigned) {
                    return LongChunkReader.transformTo(new CharChunkReader(options),
                            (chunk, ii) -> QueryLanguageFunctionUtils.longCast(chunk.get(ii)));
                }
                return LongChunkReader.transformTo(new ShortChunkReader(options),
                        (chunk, ii) -> maskIfOverflow(unsigned,
                                Short.BYTES, QueryLanguageFunctionUtils.longCast(chunk.get(ii))));
            case 32:
                return LongChunkReader.transformTo(new IntChunkReader(options), (chunk, ii) -> maskIfOverflow(unsigned,
                        Integer.BYTES, QueryLanguageFunctionUtils.longCast(chunk.get(ii))));
            case 64:
                // note unsigned long may overflow long; but user has asked for this
                return new LongChunkReader(options);
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkReader<WritableObjectChunk<BigInteger, Values>> intToBigInt(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Int intType = (ArrowType.Int) arrowType;
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return transformToObject(new ByteChunkReader(options), (chunk, ii) -> toBigInt(maskIfOverflow(
                        unsigned, Byte.BYTES, QueryLanguageFunctionUtils.longCast(chunk.get(ii)))));
            case 16:
                if (unsigned) {
                    return transformToObject(new CharChunkReader(options),
                            (chunk, ii) -> toBigInt(QueryLanguageFunctionUtils.longCast(chunk.get(ii))));
                }
                return transformToObject(new ShortChunkReader(options), (chunk, ii) -> toBigInt(maskIfOverflow(
                        unsigned, Short.BYTES, QueryLanguageFunctionUtils.longCast(chunk.get(ii)))));
            case 32:
                return transformToObject(new IntChunkReader(options), (chunk, ii) -> toBigInt(maskIfOverflow(
                        unsigned, Integer.BYTES, QueryLanguageFunctionUtils.longCast(chunk.get(ii)))));
            case 64:
                return transformToObject(new LongChunkReader(options),
                        (chunk, ii) -> maskIfOverflow(unsigned, Long.BYTES, toBigInt(chunk.get(ii))));
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkReader<WritableFloatChunk<Values>> intToFloat(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Int intType = (ArrowType.Int) arrowType;
        final int bitWidth = intType.getBitWidth();
        final boolean signed = intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return FloatChunkReader.transformTo(new ByteChunkReader(options),
                        (chunk, ii) -> floatCast(Byte.BYTES, signed, chunk.isNull(ii), chunk.get(ii)));
            case 16:
                if (!signed) {
                    return FloatChunkReader.transformTo(new CharChunkReader(options),
                            (chunk, ii) -> floatCast(Character.BYTES, signed, chunk.isNull(ii), chunk.get(ii)));
                }
                return FloatChunkReader.transformTo(new ShortChunkReader(options),
                        (chunk, ii) -> floatCast(Short.BYTES, signed, chunk.isNull(ii), chunk.get(ii)));
            case 32:
                return FloatChunkReader.transformTo(new IntChunkReader(options),
                        (chunk, ii) -> floatCast(Integer.BYTES, signed, chunk.isNull(ii), chunk.get(ii)));
            case 64:
                return FloatChunkReader.transformTo(new LongChunkReader(options),
                        (chunk, ii) -> floatCast(Long.BYTES, signed, chunk.isNull(ii), chunk.get(ii)));
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static float floatCast(
            final int numBytes,
            boolean signed,
            boolean isNull,
            long value) {
        if (isNull) {
            // note that we widen the value without proper null handling
            return QueryConstants.NULL_FLOAT;
        }
        if (signed) {
            return QueryLanguageFunctionUtils.floatCast(value);
        }

        if (numBytes == Long.BYTES) {
            long lo = value & ((1L << 32) - 1);
            long hi = (value >> 32) & ((1L << 32) - 1);
            return ((float) hi) * 2e32f + (float) lo;
        }

        // can mask in place
        value &= (1L << (numBytes * Byte.SIZE)) - 1;
        return QueryLanguageFunctionUtils.floatCast(value);
    }

    private static ChunkReader<WritableDoubleChunk<Values>> intToDouble(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Int intType = (ArrowType.Int) arrowType;
        final int bitWidth = intType.getBitWidth();
        final boolean signed = intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return DoubleChunkReader.transformTo(new ByteChunkReader(options),
                        (chunk, ii) -> doubleCast(Byte.BYTES, signed, chunk.isNull(ii), chunk.get(ii)));
            case 16:
                if (!signed) {
                    return DoubleChunkReader.transformTo(new CharChunkReader(options),
                            (chunk, ii) -> doubleCast(Character.BYTES, signed, chunk.isNull(ii), chunk.get(ii)));
                }
                return DoubleChunkReader.transformTo(new ShortChunkReader(options),
                        (chunk, ii) -> doubleCast(Short.BYTES, signed, chunk.isNull(ii), chunk.get(ii)));
            case 32:
                return DoubleChunkReader.transformTo(new IntChunkReader(options),
                        (chunk, ii) -> doubleCast(Integer.BYTES, signed, chunk.isNull(ii), chunk.get(ii)));
            case 64:
                return DoubleChunkReader.transformTo(new LongChunkReader(options),
                        (chunk, ii) -> doubleCast(Long.BYTES, signed, chunk.isNull(ii), chunk.get(ii)));
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static double doubleCast(
            final int numBytes,
            boolean signed,
            boolean isNull,
            long value) {
        if (isNull) {
            // note that we widen the value without proper null handling
            return QueryConstants.NULL_DOUBLE;
        }
        if (signed) {
            return QueryLanguageFunctionUtils.doubleCast(value);
        }

        if (numBytes == Long.BYTES) {
            long lo = value & ((1L << 32) - 1);
            long hi = (value >> 32) & ((1L << 32) - 1);
            return ((double) hi) * 2e32 + (double) lo;
        }

        // can mask in place
        value &= (1L << (numBytes * Byte.SIZE)) - 1;
        return QueryLanguageFunctionUtils.doubleCast(value);
    }

    private static ChunkReader<WritableObjectChunk<BigDecimal, Values>> intToBigDecimal(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Int intType = (ArrowType.Int) arrowType;
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return transformToObject(new ByteChunkReader(options), (chunk, ii) -> toBigDecimal(maskIfOverflow(
                        unsigned, Byte.BYTES, QueryLanguageFunctionUtils.longCast(chunk.get(ii)))));
            case 16:
                if (unsigned) {
                    return transformToObject(new CharChunkReader(options), (chunk, ii) -> toBigDecimal(maskIfOverflow(
                            unsigned, Character.BYTES, QueryLanguageFunctionUtils.longCast(chunk.get(ii)))));
                }
                return transformToObject(new ShortChunkReader(options), (chunk, ii) -> toBigDecimal(maskIfOverflow(
                        unsigned, Short.BYTES, QueryLanguageFunctionUtils.longCast(chunk.get(ii)))));
            case 32:
                return transformToObject(new IntChunkReader(options), (chunk, ii) -> toBigDecimal(maskIfOverflow(
                        unsigned, Integer.BYTES, QueryLanguageFunctionUtils.longCast(chunk.get(ii)))));
            case 64:
                return transformToObject(new LongChunkReader(options), (chunk, ii) -> {
                    final BigInteger bi = maskIfOverflow(unsigned, Long.BYTES, toBigInt(chunk.get(ii)));
                    return bi == null ? null : new BigDecimal(bi);
                });
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkReader<WritableCharChunk<Values>> intToChar(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.Int intType = (ArrowType.Int) arrowType;
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return CharChunkReader.transformTo(new ByteChunkReader(options),
                        (chunk, ii) -> QueryLanguageFunctionUtils.charCast(chunk.get(ii)));
            case 16:
                if (unsigned) {
                    return new CharChunkReader(options);
                } else {
                    return CharChunkReader.transformTo(new ShortChunkReader(options),
                            (chunk, ii) -> QueryLanguageFunctionUtils.charCast(chunk.get(ii)));
                }
            case 32:
                // note int mappings to char will overflow; but user has asked for this
                return CharChunkReader.transformTo(new IntChunkReader(options),
                        (chunk, ii) -> QueryLanguageFunctionUtils.charCast(chunk.get(ii)));
            case 64:
                // note long mappings to short will overflow; but user has asked for this
                return CharChunkReader.transformTo(new LongChunkReader(options),
                        (chunk, ii) -> QueryLanguageFunctionUtils.charCast(chunk.get(ii)));
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkReader<WritableByteChunk<Values>> boolToBoolean(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        return new BooleanChunkReader();
    }

    private static ChunkReader<WritableObjectChunk<byte[], Values>> fixedSizeBinaryToByteArray(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        final ArrowType.FixedSizeBinary fixedSizeBinary = (ArrowType.FixedSizeBinary) arrowType;
        final int elementWidth = fixedSizeBinary.getByteWidth();
        return new FixedWidthChunkReader<>(elementWidth, false, options, (dataInput) -> {
            final byte[] value = new byte[elementWidth];
            dataInput.readFully(value);
            return value;
        });
    }

    private static ChunkReader<WritableIntChunk<Values>> dateToInt(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        // see dateToLocalDate's comment for more information on wire format
        final ArrowType.Date dateType = (ArrowType.Date) arrowType;
        switch (dateType.getUnit()) {
            case DAY:
                return new IntChunkReader(options);
            case MILLISECOND:
                final long factor = Duration.ofDays(1).toMillis();
                return IntChunkReader.transformTo(new LongChunkReader(options), (chunk, ii) -> {
                    long value = chunk.get(ii);
                    return value == QueryConstants.NULL_LONG ? QueryConstants.NULL_INT : (int) (value / factor);
                });
            default:
                throw new IllegalArgumentException("Unexpected date unit: " + dateType.getUnit());
        }
    }

    private static ChunkReader<WritableLongChunk<Values>> dateToLong(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        // see dateToLocalDate's comment for more information on wire format
        final ArrowType.Date dateType = (ArrowType.Date) arrowType;
        switch (dateType.getUnit()) {
            case DAY:
                return LongChunkReader.transformTo(new IntChunkReader(options),
                        (chunk, ii) -> QueryLanguageFunctionUtils.longCast(chunk.get(ii)));
            case MILLISECOND:
                final long factor = Duration.ofDays(1).toMillis();
                return LongChunkReader.transformTo(new LongChunkReader(options), (chunk, ii) -> {
                    long value = chunk.get(ii);
                    return value == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : value / factor;
                });
            default:
                throw new IllegalArgumentException("Unexpected date unit: " + dateType.getUnit());
        }
    }

    private static ChunkReader<WritableObjectChunk<LocalDate, Values>> dateToLocalDate(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        /*
         * Date is either a 32-bit or 64-bit signed integer type representing an elapsed time since UNIX epoch
         * (1970-01-01), stored in either of two units:
         *
         * @formatter:off
         * - Milliseconds (64 bits) indicating UNIX time elapsed since the epoch (no leap seconds), where the values are
         * evenly divisible by 86400000
         * - Days (32 bits) since the UNIX epoch
         * @formatter:on
         */
        final ArrowType.Date dateType = (ArrowType.Date) arrowType;
        switch (dateType.getUnit()) {
            case DAY:
                return transformToObject(new IntChunkReader(options), (chunk, ii) -> {
                    int value = chunk.get(ii);
                    return value == QueryConstants.NULL_INT ? null : DateTimeUtils.epochDaysToLocalDate(value);
                });
            case MILLISECOND:
                final long factor = Duration.ofDays(1).toMillis();
                return transformToObject(new LongChunkReader(options), (chunk, ii) -> {
                    long value = chunk.get(ii);
                    return value == QueryConstants.NULL_LONG
                            ? null
                            : DateTimeUtils.epochDaysToLocalDate(value / factor);
                });
            default:
                throw new IllegalArgumentException("Unexpected date unit: " + dateType.getUnit());
        }
    }

    private static ChunkReader<WritableLongChunk<Values>> intervalToDurationLong(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        // See intervalToPeriod's comment for more information on wire format.

        final ArrowType.Interval intervalType = (ArrowType.Interval) arrowType;
        switch (intervalType.getUnit()) {
            case YEAR_MONTH:
            case MONTH_DAY_NANO:
                throw new IllegalArgumentException(String.format(
                        "Do not support %s interval to Duration as long conversion", intervalType));

            case DAY_TIME:
                return LongChunkReader
                        .transformTo(new FixedWidthChunkReader<>(Integer.BYTES * 2, false, options, dataInput -> {
                            final int days = dataInput.readInt();
                            final int millis = dataInput.readInt();
                            return Duration.ofDays(days).plusMillis(millis);
                        }), (chunk, ii) -> {
                            final Duration value = chunk.get(ii);
                            return value == null ? QueryConstants.NULL_LONG : value.toNanos();
                        });

            default:
                throw new IllegalArgumentException("Unexpected interval unit: " + intervalType.getUnit());
        }
    }

    private static ChunkReader<WritableObjectChunk<Duration, Values>> intervalToDuration(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        // See intervalToPeriod's comment for more information on wire format.

        final ArrowType.Interval intervalType = (ArrowType.Interval) arrowType;
        switch (intervalType.getUnit()) {
            case YEAR_MONTH:
            case MONTH_DAY_NANO:
                throw new IllegalArgumentException(String.format(
                        "Do not support %s interval to Duration conversion", intervalType));

            case DAY_TIME:
                return new FixedWidthChunkReader<>(Integer.BYTES * 2 + Long.BYTES, false, options, dataInput -> {
                    final int days = dataInput.readInt();
                    final int millis = dataInput.readInt();
                    return Duration.ofDays(days).plusMillis(millis);
                });

            default:
                throw new IllegalArgumentException("Unexpected interval unit: " + intervalType.getUnit());
        }
    }

    private static ChunkReader<WritableObjectChunk<Period, Values>> intervalToPeriod(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        /*
         * A "calendar" interval which models types that don't necessarily have a precise duration without the context
         * of a base timestamp (e.g. days can differ in length during day light savings time transitions). All integers
         * in the types below are stored in the endianness indicated by the schema.
         *
         * @formatter:off
         * YEAR_MONTH:
         * Indicates the number of elapsed whole months, stored as 4-byte signed integers.
         *
         * DAY_TIME:
         * Indicates the number of elapsed days and milliseconds (no leap seconds), stored as 2 contiguous 32-bit signed
         * integers (8-bytes in total).
         *
         * MONTH_DAY_NANO:
         * A triple of the number of elapsed months, days, and nanoseconds. The values are stored
         * contiguously in 16-byte blocks. Months and days are encoded as 32-bit signed integers and nanoseconds is
         * encoded as a 64-bit signed integer. Nanoseconds does not allow for leap seconds.
         * @formatter:on
         *
         * Note: Period does not handle the time portion of DAY_TIME and MONTH_DAY_NANO. Arrow stores these in
         * PeriodDuration pairs.
         */
        final ArrowType.Interval intervalType = (ArrowType.Interval) arrowType;
        switch (intervalType.getUnit()) {
            case YEAR_MONTH:
                return transformToObject(new IntChunkReader(options), (chunk, ii) -> {
                    int value = chunk.get(ii);
                    return value == QueryConstants.NULL_INT ? null : Period.ofMonths(value);
                });
            case DAY_TIME:
                final long factor = Duration.ofDays(1).toMillis();
                return new FixedWidthChunkReader<>(Integer.BYTES * 2, false, options, dataInput -> {
                    final int days = dataInput.readInt();
                    final int millis = dataInput.readInt();
                    return Period.ofDays(days).plusDays(millis / factor);
                });
            case MONTH_DAY_NANO:
                final long nsPerDay = Duration.ofDays(1).toNanos();
                return new FixedWidthChunkReader<>(Integer.BYTES * 2 + Long.BYTES, false, options, dataInput -> {
                    final int months = dataInput.readInt();
                    final int days = dataInput.readInt();
                    final long nanos = dataInput.readLong();
                    return Period.of(0, months, days).plusDays(nanos / (nsPerDay));
                });
            default:
                throw new IllegalArgumentException("Unexpected interval unit: " + intervalType.getUnit());
        }
    }

    private static ChunkReader<WritableObjectChunk<PeriodDuration, Values>> intervalToPeriodDuration(
            final ArrowType arrowType,
            final BarrageTypeInfo<Field> typeInfo,
            final BarrageOptions options) {
        // See intervalToPeriod's comment for more information on wire format.

        final ArrowType.Interval intervalType = (ArrowType.Interval) arrowType;
        switch (intervalType.getUnit()) {
            case YEAR_MONTH:
                return transformToObject(new IntChunkReader(options), (chunk, ii) -> {
                    int value = chunk.get(ii);
                    return value == QueryConstants.NULL_INT
                            ? null
                            : new PeriodDuration(Period.ofMonths(value), Duration.ZERO);
                });
            case DAY_TIME:
                return new FixedWidthChunkReader<>(Integer.BYTES * 2, false, options, dataInput -> {
                    final int days = dataInput.readInt();
                    final int millis = dataInput.readInt();
                    return new PeriodDuration(Period.ofDays(days), Duration.ofMillis(millis));
                });
            case MONTH_DAY_NANO:
                return new FixedWidthChunkReader<>(Integer.BYTES * 2 + Long.BYTES, false, options, dataInput -> {
                    final int months = dataInput.readInt();
                    final int days = dataInput.readInt();
                    final long nanos = dataInput.readLong();
                    return new PeriodDuration(Period.of(0, months, days), Duration.ofNanos(nanos));
                });
            default:
                throw new IllegalArgumentException("Unexpected interval unit: " + intervalType.getUnit());
        }
    }

    private static BigInteger toBigInt(final long value) {
        return value == QueryConstants.NULL_LONG ? null : BigInteger.valueOf(value);
    }

    private static BigDecimal toBigDecimal(final long value) {
        return value == QueryConstants.NULL_LONG ? null : BigDecimal.valueOf(value);
    }

    /**
     * Applies a mask to handle overflow for unsigned values by constraining the value to the range that can be
     * represented with the specified number of bytes.
     * <p>
     * This method ensures that negative values (in the case of unsigned inputs) are masked to fit within the valid
     * range for the given number of bytes, effectively wrapping them around to their equivalent unsigned
     * representation.
     * <p>
     * Special handling is included to preserve the value of null-equivalent constants and to skip masking for signed
     * values.
     * <p>
     * Note that short can only be sign extended from byte so we don't need to consider other numByte configurations.
     *
     * @param unsigned Whether the value should be treated as unsigned.
     * @param value The input value to potentially mask.
     * @return The masked value if unsigned and overflow occurs; otherwise, the original value.
     */
    @SuppressWarnings("SameParameterValue")
    private static short maskIfOverflow(final boolean unsigned, short value) {
        if (unsigned && value != QueryConstants.NULL_SHORT) {
            value &= (short) ((1L << 8) - 1);
        }
        return value;
    }

    /**
     * Applies a mask to handle overflow for unsigned values by constraining the value to the range that can be
     * represented with the specified number of bytes.
     * <p>
     * This method ensures that negative values (in the case of unsigned inputs) are masked to fit within the valid
     * range for the given number of bytes, effectively wrapping them around to their equivalent unsigned
     * representation.
     * <p>
     * Special handling is included to preserve the value of null-equivalent constants and to skip masking for signed
     * values.
     *
     * @param unsigned Whether the value should be treated as unsigned.
     * @param numBytes The number of bytes to constrain the value to (e.g., 1 for byte, 2 for short).
     * @param value The input value to potentially mask.
     * @return The masked value if unsigned and overflow occurs; otherwise, the original value.
     */
    private static int maskIfOverflow(final boolean unsigned, final int numBytes, int value) {
        if (unsigned && value != QueryConstants.NULL_INT) {
            value &= (int) ((1L << (numBytes * 8)) - 1);
        }
        return value;
    }

    /**
     * Applies a mask to handle overflow for unsigned values by constraining the value to the range that can be
     * represented with the specified number of bytes.
     * <p>
     * This method ensures that negative values (in the case of unsigned inputs) are masked to fit within the valid
     * range for the given number of bytes, effectively wrapping them around to their equivalent unsigned
     * representation.
     * <p>
     * Special handling is included to preserve the value of null-equivalent constants and to skip masking for signed
     * values.
     *
     * @param unsigned Whether the value should be treated as unsigned.
     * @param numBytes The number of bytes to constrain the value to (e.g., 1 for byte, 2 for short).
     * @param value The input value to potentially mask.
     * @return The masked value if unsigned and overflow occurs; otherwise, the original value.
     */
    private static long maskIfOverflow(final boolean unsigned, final int numBytes, long value) {
        if (unsigned && value != QueryConstants.NULL_LONG) {
            value &= ((1L << (numBytes * 8)) - 1);
        }
        return value;
    }

    /**
     * Applies a mask to handle overflow for unsigned values by constraining the value to the range that can be
     * represented with the specified number of bytes.
     * <p>
     * This method ensures that negative values (in the case of unsigned inputs) are masked to fit within the valid
     * range for the given number of bytes, effectively wrapping them around to their equivalent unsigned
     * representation.
     * <p>
     * Special handling is included to preserve the value of null-equivalent constants and to skip masking for signed
     * values.
     *
     * @param unsigned Whether the value should be treated as unsigned.
     * @param numBytes The number of bytes to constrain the value to (e.g., 1 for byte, 2 for short).
     * @param value The input value to potentially mask.
     * @return The masked value if unsigned and overflow occurs; otherwise, the original value.
     */
    @SuppressWarnings("SameParameterValue")
    private static BigInteger maskIfOverflow(final boolean unsigned, final int numBytes, final BigInteger value) {
        if (unsigned && value != null && value.compareTo(BigInteger.ZERO) < 0) {
            return value.and(BigInteger.ONE.shiftLeft(numBytes * 8).subtract(BigInteger.ONE));
        }
        return value;
    }

    private interface ToObjectTransformFunction<T, WIRE_CHUNK_TYPE extends WritableChunk<Values>> {
        T get(WIRE_CHUNK_TYPE wireValues, int wireOffset);
    }

    private static <T, WIRE_CHUNK_TYPE extends WritableChunk<Values>, CR extends ChunkReader<WIRE_CHUNK_TYPE>> ChunkReader<WritableObjectChunk<T, Values>> transformToObject(
            final CR wireReader,
            final ToObjectTransformFunction<T, WIRE_CHUNK_TYPE> wireTransform) {
        return new TransformingChunkReader<>(
                wireReader,
                WritableObjectChunk::makeWritableChunk,
                WritableChunk::asWritableObjectChunk,
                (wireValues, outChunk, wireOffset, outOffset) -> outChunk.set(
                        outOffset, wireTransform.get(wireValues, wireOffset)));
    }
}
