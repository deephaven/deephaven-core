//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.engine.table.impl.preview.ArrayPreview;
import io.deephaven.engine.table.impl.preview.DisplayWrapper;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.extensions.barrage.chunk.array.ArrayExpansionKernel;
import io.deephaven.extensions.barrage.chunk.vector.VectorExpansionKernel;
import io.deephaven.extensions.barrage.util.ArrowIpcUtil;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.Float16;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyObject;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * JVM implementation of {@link ChunkWriter.Factory}, suitable for use in Java clients and servers. This default
 * implementation may not round trip flight types in a stable way, but will round trip Deephaven table definitions and
 * table data. Neither of these is a required/expected property of being a Flight/Barrage/Deephaven client.
 */
public class DefaultChunkWriterFactory implements ChunkWriter.Factory {
    public static final Logger log = LoggerFactory.getLogger(DefaultChunkWriterFactory.class);
    public static final ChunkWriter.Factory INSTANCE = new DefaultChunkWriterFactory();

    /**
     * This supplier interface simplifies the cost to operate off of the ArrowType directly since the Arrow POJO is not
     * yet supported over GWT.
     */
    protected interface ArrowTypeChunkWriterSupplier {
        ChunkWriter<? extends Chunk<Values>> make(
                final BarrageTypeInfo<Field> typeInfo);
    }

    private boolean toStringUnknownTypes = true;
    private final Map<ArrowType.ArrowTypeID, Map<Class<?>, ArrowTypeChunkWriterSupplier>> registeredFactories =
            new EnumMap<>(ArrowType.ArrowTypeID.class);

    protected DefaultChunkWriterFactory() {
        register(ArrowType.ArrowTypeID.Timestamp, long.class, DefaultChunkWriterFactory::timestampFromLong);
        register(ArrowType.ArrowTypeID.Timestamp, Instant.class, DefaultChunkWriterFactory::timestampFromInstant);
        register(ArrowType.ArrowTypeID.Timestamp, ZonedDateTime.class,
                DefaultChunkWriterFactory::timestampFromZonedDateTime);
        register(ArrowType.ArrowTypeID.Utf8, String.class, DefaultChunkWriterFactory::utf8FromString);
        register(ArrowType.ArrowTypeID.Utf8, Object.class, DefaultChunkWriterFactory::utf8FromObject);
        register(ArrowType.ArrowTypeID.Utf8, PyObject.class, DefaultChunkWriterFactory::utf8FromPyObject);
        register(ArrowType.ArrowTypeID.Utf8, ArrayPreview.class, DefaultChunkWriterFactory::utf8FromObject);
        register(ArrowType.ArrowTypeID.Utf8, DisplayWrapper.class, DefaultChunkWriterFactory::utf8FromObject);
        register(ArrowType.ArrowTypeID.Duration, long.class, DefaultChunkWriterFactory::durationFromLong);
        register(ArrowType.ArrowTypeID.Duration, Duration.class, DefaultChunkWriterFactory::durationFromDuration);
        register(ArrowType.ArrowTypeID.FloatingPoint, float.class, DefaultChunkWriterFactory::floatingPointFromFloat);
        register(ArrowType.ArrowTypeID.FloatingPoint, double.class,
                DefaultChunkWriterFactory::floatingPointFromDouble);
        register(ArrowType.ArrowTypeID.FloatingPoint, BigDecimal.class,
                DefaultChunkWriterFactory::floatingPointFromBigDecimal);
        register(ArrowType.ArrowTypeID.Binary, byte[].class, DefaultChunkWriterFactory::binaryFromByteArray);
        register(ArrowType.ArrowTypeID.Binary, BigInteger.class, DefaultChunkWriterFactory::binaryFromBigInt);
        register(ArrowType.ArrowTypeID.Binary, BigDecimal.class, DefaultChunkWriterFactory::binaryFromBigDecimal);
        register(ArrowType.ArrowTypeID.Binary, Schema.class, DefaultChunkWriterFactory::binaryFromSchema);
        register(ArrowType.ArrowTypeID.Time, long.class, DefaultChunkWriterFactory::timeFromLong);
        register(ArrowType.ArrowTypeID.Time, LocalTime.class, DefaultChunkWriterFactory::timeFromLocalTime);
        register(ArrowType.ArrowTypeID.Decimal, byte.class, DefaultChunkWriterFactory::decimalFromByte);
        register(ArrowType.ArrowTypeID.Decimal, char.class, DefaultChunkWriterFactory::decimalFromChar);
        register(ArrowType.ArrowTypeID.Decimal, short.class, DefaultChunkWriterFactory::decimalFromShort);
        register(ArrowType.ArrowTypeID.Decimal, int.class, DefaultChunkWriterFactory::decimalFromInt);
        register(ArrowType.ArrowTypeID.Decimal, long.class, DefaultChunkWriterFactory::decimalFromLong);
        register(ArrowType.ArrowTypeID.Decimal, BigInteger.class, DefaultChunkWriterFactory::decimalFromBigInteger);
        register(ArrowType.ArrowTypeID.Decimal, float.class, DefaultChunkWriterFactory::decimalFromFloat);
        register(ArrowType.ArrowTypeID.Decimal, double.class, DefaultChunkWriterFactory::decimalFromDouble);
        register(ArrowType.ArrowTypeID.Decimal, BigDecimal.class, DefaultChunkWriterFactory::decimalFromBigDecimal);
        register(ArrowType.ArrowTypeID.Int, byte.class, DefaultChunkWriterFactory::intFromByte);
        register(ArrowType.ArrowTypeID.Int, char.class, DefaultChunkWriterFactory::intFromChar);
        register(ArrowType.ArrowTypeID.Int, short.class, DefaultChunkWriterFactory::intFromShort);
        register(ArrowType.ArrowTypeID.Int, int.class, DefaultChunkWriterFactory::intFromInt);
        register(ArrowType.ArrowTypeID.Int, long.class, DefaultChunkWriterFactory::intFromLong);
        register(ArrowType.ArrowTypeID.Int, BigInteger.class, DefaultChunkWriterFactory::intFromObject);
        register(ArrowType.ArrowTypeID.Int, float.class, DefaultChunkWriterFactory::intFromFloat);
        register(ArrowType.ArrowTypeID.Int, double.class, DefaultChunkWriterFactory::intFromDouble);
        register(ArrowType.ArrowTypeID.Int, BigDecimal.class, DefaultChunkWriterFactory::intFromObject);
        register(ArrowType.ArrowTypeID.Bool, boolean.class, DefaultChunkWriterFactory::boolFromBoolean);
        register(ArrowType.ArrowTypeID.Bool, Boolean.class, DefaultChunkWriterFactory::boolFromBoolean);
        register(ArrowType.ArrowTypeID.Bool, byte.class, DefaultChunkWriterFactory::boolFromBoolean);
        register(ArrowType.ArrowTypeID.FixedSizeBinary, byte[].class,
                DefaultChunkWriterFactory::fixedSizeBinaryFromByteArray);
        register(ArrowType.ArrowTypeID.Date, int.class, DefaultChunkWriterFactory::dateFromInt);
        register(ArrowType.ArrowTypeID.Date, long.class, DefaultChunkWriterFactory::dateFromLong);
        register(ArrowType.ArrowTypeID.Date, LocalDate.class, DefaultChunkWriterFactory::dateFromLocalDate);
        register(ArrowType.ArrowTypeID.Interval, long.class, DefaultChunkWriterFactory::intervalFromDurationLong);
        register(ArrowType.ArrowTypeID.Interval, Duration.class, DefaultChunkWriterFactory::intervalFromDuration);
        register(ArrowType.ArrowTypeID.Interval, Period.class, DefaultChunkWriterFactory::intervalFromPeriod);
        register(ArrowType.ArrowTypeID.Interval, PeriodDuration.class,
                DefaultChunkWriterFactory::intervalFromPeriodDuration);
    }

    public void disableToStringUnknownTypes() {
        toStringUnknownTypes = false;
    }

    @Override
    public <T extends Chunk<Values>> ChunkWriter<T> newWriter(
            @NotNull final BarrageTypeInfo<org.apache.arrow.flatbuf.Field> typeInfo) {
        BarrageTypeInfo<Field> fieldTypeInfo = new BarrageTypeInfo<>(
                typeInfo.type(),
                typeInfo.componentType(),
                Field.convertField(typeInfo.arrowField()));
        return newWriterPojo(fieldTypeInfo);
    }

    public <T extends Chunk<Values>> ChunkWriter<T> newWriterPojo(
            @NotNull final BarrageTypeInfo<Field> typeInfo) {
        // TODO (deephaven/deephaven-core#6033): Run-End Support
        // TODO (deephaven/deephaven-core#6034): Dictionary Support

        final Field field = typeInfo.arrowField();

        final ArrowType.ArrowTypeID typeId = field.getType().getTypeID();
        final boolean isSpecialType = DefaultChunkReaderFactory.SPECIAL_TYPES.contains(typeId);

        // Note we do not support these as they require 64-bit offsets:
        if (typeId == ArrowType.ArrowTypeID.LargeUtf8
                || typeId == ArrowType.ArrowTypeID.LargeBinary
                || typeId == ArrowType.ArrowTypeID.LargeList) {
            throw new UnsupportedOperationException(String.format(
                    "No support for 64-bit offsets to map arrow type %s from %s.",
                    field.getType().toString(),
                    typeInfo.type().getCanonicalName()));
        }

        final Map<Class<?>, ArrowTypeChunkWriterSupplier> knownWriters = registeredFactories.get(typeId);
        if (knownWriters == null && !isSpecialType) {
            throw new UnsupportedOperationException(String.format(
                    "No known ChunkWriter for arrow type %s from %s.",
                    field.getType().toString(),
                    typeInfo.type().getCanonicalName()));
        }

        final ArrowTypeChunkWriterSupplier chunkWriterFactory =
                knownWriters == null ? null : knownWriters.get(typeInfo.type());
        if (chunkWriterFactory != null) {
            // noinspection unchecked
            final ChunkWriter<T> writer = (ChunkWriter<T>) chunkWriterFactory.make(typeInfo);
            if (writer != null) {
                return writer;
            }
        }

        if (!isSpecialType) {
            if (toStringUnknownTypes) {
                // noinspection unchecked
                return (ChunkWriter<T>) new VarBinaryChunkWriter<>(
                        field.isNullable(),
                        (out, item) -> out.write(item.toString().getBytes(StandardCharsets.UTF_8)));
            }
            throw new UnsupportedOperationException(String.format(
                    "No known ChunkWriter for arrow type %s from %s. Supported types: %s",
                    field.getType().toString(),
                    typeInfo.type().getCanonicalName(),
                    knownWriters.keySet().stream().map(Object::toString).collect(Collectors.joining(", "))));
        }

        if (typeId == ArrowType.ArrowTypeID.Null) {
            // noinspection unchecked
            return (ChunkWriter<T>) NullChunkWriter.INSTANCE;
        }

        if (typeId == ArrowType.ArrowTypeID.List
                || typeId == ArrowType.ArrowTypeID.ListView
                || typeId == ArrowType.ArrowTypeID.FixedSizeList) {

            int fixedSizeLength = 0;
            final ListChunkReader.Mode mode;
            if (typeId == ArrowType.ArrowTypeID.List) {
                mode = ListChunkReader.Mode.DENSE;
            } else if (typeId == ArrowType.ArrowTypeID.ListView) {
                mode = ListChunkReader.Mode.SPARSE;
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
                        field.getChildren().get(0));
            } else if (typeInfo.type().isArray()) {
                final Class<?> componentType = typeInfo.componentType();
                // noinspection DataFlowIssue
                componentTypeInfo = new BarrageTypeInfo<>(
                        componentType,
                        componentType.getComponentType(),
                        field.getChildren().get(0));
            } else {
                throw new UnsupportedOperationException(String.format(
                        "No known ChunkWriter for arrow type %s from %s. Expected destination type to be an array.",
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
            final ChunkWriter<Chunk<Values>> componentWriter = newWriterPojo(componentTypeInfo);

            // noinspection unchecked
            return (ChunkWriter<T>) new ListChunkWriter<>(
                    mode, fixedSizeLength, kernel, componentWriter, field.isNullable());
        }

        if (typeId == ArrowType.ArrowTypeID.Map) {
            // TODO: should we allow the user to supply the collector?
            final Field structField = field.getChildren().get(0);
            final BarrageTypeInfo<Field> keyTypeInfo = BarrageUtil.getDefaultType(structField.getChildren().get(0));
            final BarrageTypeInfo<Field> valueTypeInfo = BarrageUtil.getDefaultType(structField.getChildren().get(1));

            final ChunkWriter<Chunk<Values>> keyWriter = newWriterPojo(keyTypeInfo);
            final ChunkWriter<Chunk<Values>> valueWriter = newWriterPojo(valueTypeInfo);

            // noinspection unchecked
            return (ChunkWriter<T>) new MapChunkWriter<>(
                    keyWriter, valueWriter, keyTypeInfo.chunkType(), valueTypeInfo.chunkType(), field.isNullable());
        }

        // TODO: if (typeId == ArrowType.ArrowTypeID.Struct) {
        // expose transformer API of Map<String, Chunk<Values>> -> T

        if (typeId == ArrowType.ArrowTypeID.Union) {
            final ArrowType.Union unionType = (ArrowType.Union) field.getType();

            final List<BarrageTypeInfo<Field>> childTypeInfo = field.getChildren().stream()
                    .map(BarrageUtil::getDefaultType)
                    .collect(Collectors.toList());
            final List<Class<?>> childClassMatcher = childTypeInfo.stream()
                    .map(BarrageTypeInfo::type)
                    .map(TypeUtils::getBoxedType)
                    .collect(Collectors.toList());
            final List<ChunkWriter<Chunk<Values>>> childWriters = childTypeInfo.stream()
                    .map(this::newWriterPojo)
                    .collect(Collectors.toList());
            final List<ChunkType> childChunkTypes = childTypeInfo.stream()
                    .map(BarrageTypeInfo::chunkType)
                    .collect(Collectors.toList());

            UnionChunkReader.Mode mode = unionType.getMode() == UnionMode.Sparse ? UnionChunkReader.Mode.Sparse
                    : UnionChunkReader.Mode.Dense;
            // noinspection unchecked
            return (ChunkWriter<T>) new UnionChunkWriter<>(mode, childClassMatcher, childWriters,
                    childChunkTypes);
        }

        throw new UnsupportedOperationException(String.format(
                "No known ChunkWriter for arrow type %s from %s. Arrow type supports: %s",
                field.getType().toString(),
                typeInfo.type().getCanonicalName(),
                knownWriters == null ? "none"
                        : knownWriters.keySet().stream()
                                .map(Object::toString)
                                .collect(Collectors.joining(", "))));
    }

    protected void register(
            final ArrowType.ArrowTypeID arrowType,
            final Class<?> deephavenType,
            final ArrowTypeChunkWriterSupplier chunkWriterFactory) {
        registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                .put(deephavenType, chunkWriterFactory);

        // if primitive automatically register the boxed version of this mapping, too
        if (deephavenType == byte.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Byte.class, typeInfo -> new ByteChunkWriter<ObjectChunk<Byte, Values>>(
                            ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                            (chunk, ii) -> TypeUtils.unbox(chunk.get(ii)),
                            typeInfo.arrowField().isNullable()));
        } else if (deephavenType == short.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Short.class, typeInfo -> new ShortChunkWriter<ObjectChunk<Short, Values>>(
                            ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                            (chunk, ii) -> TypeUtils.unbox(chunk.get(ii)),
                            typeInfo.arrowField().isNullable()));
        } else if (deephavenType == int.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Integer.class, typeInfo -> new IntChunkWriter<ObjectChunk<Integer, Values>>(
                            ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                            (chunk, ii) -> TypeUtils.unbox(chunk.get(ii)),
                            typeInfo.arrowField().isNullable()));
        } else if (deephavenType == long.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Long.class, typeInfo -> new LongChunkWriter<ObjectChunk<Long, Values>>(
                            ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                            (chunk, ii) -> TypeUtils.unbox(chunk.get(ii)),
                            typeInfo.arrowField().isNullable()));
        } else if (deephavenType == char.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Character.class, typeInfo -> new CharChunkWriter<ObjectChunk<Character, Values>>(
                            ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                            (chunk, ii) -> TypeUtils.unbox(chunk.get(ii)),
                            typeInfo.arrowField().isNullable()));
        } else if (deephavenType == float.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Float.class, typeInfo -> new FloatChunkWriter<ObjectChunk<Float, Values>>(
                            ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                            (chunk, ii) -> TypeUtils.unbox(chunk.get(ii)),
                            typeInfo.arrowField().isNullable()));
        } else if (deephavenType == double.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Double.class, typeInfo -> new DoubleChunkWriter<ObjectChunk<Double, Values>>(
                            ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                            (chunk, ii) -> TypeUtils.unbox(chunk.get(ii)),
                            typeInfo.arrowField().isNullable()));
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

    private static ChunkWriter<Chunk<Values>> timestampFromLong(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Timestamp tsType = (ArrowType.Timestamp) typeInfo.arrowField().getType();
        final long factor = factorForTimeUnit(tsType.getUnit());
        // TODO (https://github.com/deephaven/deephaven-core/issues/5241): Inconsistent handling of ZonedDateTime
        // we do not know whether the incoming chunk source is a LongChunk or ObjectChunk<ZonedDateTime>
        return new LongChunkWriter<>(
                (Chunk<Values> source, int offset) -> {
                    if (source instanceof LongChunk) {
                        return source.asLongChunk().isNull(offset);
                    }

                    return source.asObjectChunk().isNull(offset);
                },
                LongChunk::getEmptyChunk,
                (Chunk<Values> source, int offset) -> {
                    if (source instanceof LongChunk) {
                        final long value = source.asLongChunk().get(offset);
                        return value == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : value / factor;
                    }

                    final ZonedDateTime value = source.<ZonedDateTime>asObjectChunk().get(offset);
                    return value == null ? QueryConstants.NULL_LONG : DateTimeUtils.epochNanos(value) / factor;
                }, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ObjectChunk<Instant, Values>> timestampFromInstant(
            final BarrageTypeInfo<Field> typeInfo) {
        final long factor = factorForTimeUnit(((ArrowType.Timestamp) typeInfo.arrowField().getType()).getUnit());
        return new LongChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, (source, offset) -> {
            final Instant value = source.get(offset);
            return value == null ? QueryConstants.NULL_LONG : DateTimeUtils.epochNanos(value) / factor;
        }, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ObjectChunk<ZonedDateTime, Values>> timestampFromZonedDateTime(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Timestamp tsType = (ArrowType.Timestamp) typeInfo.arrowField().getType();
        final long factor = factorForTimeUnit(tsType.getUnit());
        return new LongChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, (source, offset) -> {
            final ZonedDateTime value = source.get(offset);
            return value == null ? QueryConstants.NULL_LONG : DateTimeUtils.epochNanos(value) / factor;
        }, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ObjectChunk<String, Values>> utf8FromString(
            final BarrageTypeInfo<Field> typeInfo) {
        return new VarBinaryChunkWriter<>(typeInfo.arrowField().isNullable(),
                (out, item) -> out.write(item.getBytes(StandardCharsets.UTF_8)));
    }

    private static ChunkWriter<ObjectChunk<Object, Values>> utf8FromObject(
            final BarrageTypeInfo<Field> typeInfo) {
        return new VarBinaryChunkWriter<>(typeInfo.arrowField().isNullable(),
                (out, item) -> out.write(item.toString().getBytes(StandardCharsets.UTF_8)));
    }

    private static ChunkWriter<ObjectChunk<PyObject, Values>> utf8FromPyObject(
            final BarrageTypeInfo<Field> typeInfo) {
        return new VarBinaryChunkWriter<>(typeInfo.arrowField().isNullable(),
                (out, item) -> out.write(item.toString().getBytes(StandardCharsets.UTF_8)));
    }

    private static ChunkWriter<LongChunk<Values>> durationFromLong(
            final BarrageTypeInfo<Field> typeInfo) {
        final long factor = factorForTimeUnit(((ArrowType.Duration) typeInfo.arrowField().getType()).getUnit());
        return factor == 1
                ? LongChunkWriter.getIdentity(typeInfo.arrowField().isNullable())
                : new LongChunkWriter<>(LongChunk::isNull, LongChunk::getEmptyChunk, (source, offset) -> {
                    final long value = source.get(offset);
                    return value == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : value / factor;
                }, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ObjectChunk<Duration, Values>> durationFromDuration(
            final BarrageTypeInfo<Field> typeInfo) {
        final long factor = factorForTimeUnit(((ArrowType.Duration) typeInfo.arrowField().getType()).getUnit());
        return new LongChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, (source, offset) -> {
            final Duration value = source.get(offset);
            return value == null ? QueryConstants.NULL_LONG : value.toNanos() / factor;
        }, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<FloatChunk<Values>> floatingPointFromFloat(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>(FloatChunk::isNull, FloatChunk::getEmptyChunk, (source, offset) -> {
                    final double value = source.get(offset);
                    return value == QueryConstants.NULL_FLOAT
                            ? QueryConstants.NULL_SHORT
                            : Float16.toFloat16((float) value);
                }, typeInfo.arrowField().isNullable());

            case SINGLE:
                return FloatChunkWriter.getIdentity(typeInfo.arrowField().isNullable());

            case DOUBLE:
                return new DoubleChunkWriter<>(FloatChunk::isNull, FloatChunk::getEmptyChunk,
                        (source, offset) -> QueryLanguageFunctionUtils.doubleCast(source.get(offset)),
                        typeInfo.arrowField().isNullable());

            default:
                throw new IllegalArgumentException("Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<DoubleChunk<Values>> floatingPointFromDouble(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>(DoubleChunk::isNull, DoubleChunk::getEmptyChunk, (source, offset) -> {
                    final double value = source.get(offset);
                    return value == QueryConstants.NULL_DOUBLE
                            ? QueryConstants.NULL_SHORT
                            : Float16.toFloat16((float) value);
                }, typeInfo.arrowField().isNullable());

            case SINGLE:
                return new FloatChunkWriter<>(DoubleChunk::isNull, DoubleChunk::getEmptyChunk,
                        (source, offset) -> QueryLanguageFunctionUtils.floatCast(source.get(offset)),
                        typeInfo.arrowField().isNullable());
            case DOUBLE:
                return DoubleChunkWriter.getIdentity(typeInfo.arrowField().isNullable());

            default:
                throw new IllegalArgumentException("Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<ObjectChunk<BigDecimal, Values>> floatingPointFromBigDecimal(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, (source, offset) -> {
                    final BigDecimal value = source.get(offset);
                    return value == null
                            ? QueryConstants.NULL_SHORT
                            : Float16.toFloat16(value.floatValue());
                }, typeInfo.arrowField().isNullable());

            case SINGLE:
                return new FloatChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                        (source, offset) -> QueryLanguageFunctionUtils.floatCast(source.get(offset)),
                        typeInfo.arrowField().isNullable());

            case DOUBLE:
                return new DoubleChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                        (source, offset) -> QueryLanguageFunctionUtils.doubleCast(source.get(offset)),
                        typeInfo.arrowField().isNullable());

            default:
                throw new IllegalArgumentException("Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<ObjectChunk<byte[], Values>> binaryFromByteArray(
            final BarrageTypeInfo<Field> typeInfo) {
        return new VarBinaryChunkWriter<>(typeInfo.arrowField().isNullable(),
                OutputStream::write);
    }

    private static ChunkWriter<ObjectChunk<BigInteger, Values>> binaryFromBigInt(
            final BarrageTypeInfo<Field> typeInfo) {
        return new VarBinaryChunkWriter<>(typeInfo.arrowField().isNullable(),
                (out, item) -> out.write(item.toByteArray()));
    }

    private static ChunkWriter<ObjectChunk<BigDecimal, Values>> binaryFromBigDecimal(
            final BarrageTypeInfo<Field> typeInfo) {
        return new VarBinaryChunkWriter<>(typeInfo.arrowField().isNullable(),
                (out, item) -> {
                    final BigDecimal normal = item.stripTrailingZeros();
                    final int v = normal.scale();
                    // Write as little endian, arrow endianness.
                    out.write(0xFF & v);
                    out.write(0xFF & (v >> 8));
                    out.write(0xFF & (v >> 16));
                    out.write(0xFF & (v >> 24));
                    out.write(normal.unscaledValue().toByteArray());
                });
    }

    private static ChunkWriter<ObjectChunk<Schema, Values>> binaryFromSchema(
            final BarrageTypeInfo<Field> typeInfo) {
        return new VarBinaryChunkWriter<>(typeInfo.arrowField().isNullable(),
                ArrowIpcUtil::serialize);
    }

    private static ChunkWriter<LongChunk<Values>> timeFromLong(
            final BarrageTypeInfo<Field> typeInfo) {
        // See timeFromLocalTime's comment for more information on wire format.
        final ArrowType.Time timeType = (ArrowType.Time) typeInfo.arrowField().getType();
        final int bitWidth = timeType.getBitWidth();
        final long factor = factorForTimeUnit(timeType.getUnit());
        switch (bitWidth) {
            case 32:
                return new IntChunkWriter<>(LongChunk::isNull, LongChunk::getEmptyChunk, (chunk, ii) -> {
                    // note: do math prior to truncation
                    long value = chunk.get(ii);
                    value = value == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : value / factor;
                    return QueryLanguageFunctionUtils.intCast(value);
                }, typeInfo.arrowField().isNullable());

            case 64:
                return new LongChunkWriter<>(LongChunk::isNull, LongChunk::getEmptyChunk, (chunk, ii) -> {
                    long value = chunk.get(ii);
                    return value == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : value / factor;
                }, typeInfo.arrowField().isNullable());

            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<ObjectChunk<LocalTime, Values>> timeFromLocalTime(
            final BarrageTypeInfo<Field> typeInfo) {
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

        final ArrowType.Time timeType = (ArrowType.Time) typeInfo.arrowField().getType();
        final int bitWidth = timeType.getBitWidth();
        final long factor = factorForTimeUnit(timeType.getUnit());
        switch (bitWidth) {
            case 32:
                return new IntChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, (chunk, ii) -> {
                    // note: do math prior to truncation
                    final LocalTime lt = chunk.get(ii);
                    final long value = lt == null ? QueryConstants.NULL_LONG : lt.toNanoOfDay() / factor;
                    return QueryLanguageFunctionUtils.intCast(value);
                }, typeInfo.arrowField().isNullable());

            case 64:
                return new LongChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, (chunk, ii) -> {
                    final LocalTime lt = chunk.get(ii);
                    return lt == null ? QueryConstants.NULL_LONG : lt.toNanoOfDay() / factor;
                }, typeInfo.arrowField().isNullable());

            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<ByteChunk<Values>> decimalFromByte(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();
        final byte[] nullValue = new byte[byteWidth];
        // note that BigInteger's byte array requires one sign bit; note we negate so the BigInteger#and keeps sign
        final BigInteger truncationMask = BigInteger.ONE.shiftLeft(byteWidth * 8 - 1)
                .subtract(BigInteger.ONE)
                .negate();

        return new FixedWidthChunkWriter<>(ByteChunk::isNull, ByteChunk::getEmptyChunk, byteWidth, false,
                typeInfo.arrowField().isNullable(),
                (out, chunk, offset) -> {
                    byte value = chunk.get(offset);
                    if (value == QueryConstants.NULL_BYTE) {
                        out.write(nullValue);
                        return;
                    }

                    writeBigDecimal(out, BigDecimal.valueOf(value), byteWidth, scale, truncationMask, nullValue);
                });
    }

    private static ChunkWriter<CharChunk<Values>> decimalFromChar(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();
        final byte[] nullValue = new byte[byteWidth];
        // note that BigInteger's byte array requires one sign bit; note we negate so the BigInteger#and keeps sign
        final BigInteger truncationMask = BigInteger.ONE.shiftLeft(byteWidth * 8 - 1)
                .subtract(BigInteger.ONE)
                .negate();

        return new FixedWidthChunkWriter<>(CharChunk::isNull, CharChunk::getEmptyChunk, byteWidth, false,
                typeInfo.arrowField().isNullable(),
                (out, chunk, offset) -> {
                    char value = chunk.get(offset);
                    if (value == QueryConstants.NULL_CHAR) {
                        out.write(nullValue);
                        return;
                    }

                    writeBigDecimal(out, BigDecimal.valueOf(value), byteWidth, scale, truncationMask, nullValue);
                });
    }

    private static ChunkWriter<ShortChunk<Values>> decimalFromShort(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();
        final byte[] nullValue = new byte[byteWidth];
        // note that BigInteger's byte array requires one sign bit; note we negate so the BigInteger#and keeps sign
        final BigInteger truncationMask = BigInteger.ONE.shiftLeft(byteWidth * 8 - 1)
                .subtract(BigInteger.ONE)
                .negate();

        return new FixedWidthChunkWriter<>(ShortChunk::isNull, ShortChunk::getEmptyChunk, byteWidth, false,
                typeInfo.arrowField().isNullable(),
                (out, chunk, offset) -> {
                    short value = chunk.get(offset);
                    if (value == QueryConstants.NULL_SHORT) {
                        out.write(nullValue);
                        return;
                    }

                    writeBigDecimal(out, BigDecimal.valueOf(value), byteWidth, scale, truncationMask, nullValue);
                });
    }

    private static ChunkWriter<IntChunk<Values>> decimalFromInt(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();
        final byte[] nullValue = new byte[byteWidth];
        // note that BigInteger's byte array requires one sign bit; note we negate so the BigInteger#and keeps sign
        final BigInteger truncationMask = BigInteger.ONE.shiftLeft(byteWidth * 8 - 1)
                .subtract(BigInteger.ONE)
                .negate();

        return new FixedWidthChunkWriter<>(IntChunk::isNull, IntChunk::getEmptyChunk, byteWidth, false,
                typeInfo.arrowField().isNullable(),
                (out, chunk, offset) -> {
                    int value = chunk.get(offset);
                    if (value == QueryConstants.NULL_INT) {
                        out.write(nullValue);
                        return;
                    }

                    writeBigDecimal(out, BigDecimal.valueOf(value), byteWidth, scale, truncationMask, nullValue);
                });
    }

    private static ChunkWriter<LongChunk<Values>> decimalFromLong(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();
        final byte[] nullValue = new byte[byteWidth];
        // note that BigInteger's byte array requires one sign bit; note we negate so the BigInteger#and keeps sign
        final BigInteger truncationMask = BigInteger.ONE.shiftLeft(byteWidth * 8 - 1)
                .subtract(BigInteger.ONE)
                .negate();

        return new FixedWidthChunkWriter<>(LongChunk::isNull, LongChunk::getEmptyChunk, byteWidth, false,
                typeInfo.arrowField().isNullable(),
                (out, chunk, offset) -> {
                    long value = chunk.get(offset);
                    if (value == QueryConstants.NULL_LONG) {
                        out.write(nullValue);
                        return;
                    }

                    writeBigDecimal(out, BigDecimal.valueOf(value), byteWidth, scale, truncationMask, nullValue);
                });
    }

    private static ChunkWriter<ObjectChunk<BigInteger, Values>> decimalFromBigInteger(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();
        final byte[] nullValue = new byte[byteWidth];
        // note that BigInteger's byte array requires one sign bit; note we negate so the BigInteger#and keeps sign
        final BigInteger truncationMask = BigInteger.ONE.shiftLeft(byteWidth * 8 - 1)
                .subtract(BigInteger.ONE)
                .negate();

        return new FixedWidthChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, byteWidth, false,
                typeInfo.arrowField().isNullable(),
                (out, chunk, offset) -> {
                    BigInteger value = chunk.get(offset);
                    if (value == null) {
                        out.write(nullValue);
                        return;
                    }

                    writeBigDecimal(out, new BigDecimal(value), byteWidth, scale, truncationMask, nullValue);
                });
    }

    private static ChunkWriter<FloatChunk<Values>> decimalFromFloat(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();
        final byte[] nullValue = new byte[byteWidth];
        // note that BigInteger's byte array requires one sign bit; note we negate so the BigInteger#and keeps sign
        final BigInteger truncationMask = BigInteger.ONE.shiftLeft(byteWidth * 8 - 1)
                .subtract(BigInteger.ONE)
                .negate();

        return new FixedWidthChunkWriter<>(FloatChunk::isNull, FloatChunk::getEmptyChunk, byteWidth, false,
                typeInfo.arrowField().isNullable(),
                (out, chunk, offset) -> {
                    float value = chunk.get(offset);
                    if (value == QueryConstants.NULL_FLOAT) {
                        out.write(nullValue);
                        return;
                    }

                    writeBigDecimal(out, BigDecimal.valueOf(value), byteWidth, scale, truncationMask, nullValue);
                });
    }

    private static ChunkWriter<DoubleChunk<Values>> decimalFromDouble(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();
        final byte[] nullValue = new byte[byteWidth];
        // note that BigInteger's byte array requires one sign bit; note we negate so the BigInteger#and keeps sign
        final BigInteger truncationMask = BigInteger.ONE.shiftLeft(byteWidth * 8 - 1)
                .subtract(BigInteger.ONE)
                .negate();

        return new FixedWidthChunkWriter<>(DoubleChunk::isNull, DoubleChunk::getEmptyChunk, byteWidth, false,
                typeInfo.arrowField().isNullable(),
                (out, chunk, offset) -> {
                    double value = chunk.get(offset);
                    if (value == QueryConstants.NULL_DOUBLE) {
                        out.write(nullValue);
                        return;
                    }

                    writeBigDecimal(out, BigDecimal.valueOf(value), byteWidth, scale, truncationMask, nullValue);
                });
    }

    private static ChunkWriter<ObjectChunk<BigDecimal, Values>> decimalFromBigDecimal(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();
        final byte[] nullValue = new byte[byteWidth];
        // note that BigInteger's byte array requires one sign bit; note we negate so the BigInteger#and keeps sign
        final BigInteger truncationMask = BigInteger.ONE.shiftLeft(byteWidth * 8 - 1)
                .subtract(BigInteger.ONE)
                .negate();

        return new FixedWidthChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, byteWidth, false,
                typeInfo.arrowField().isNullable(),
                (out, chunk, offset) -> {
                    BigDecimal value = chunk.get(offset);
                    if (value == null) {
                        out.write(nullValue);
                        return;
                    }

                    writeBigDecimal(out, value, byteWidth, scale, truncationMask, nullValue);
                });
    }

    private static void writeBigDecimal(
            @NotNull final DataOutput output,
            @NotNull BigDecimal value,
            final int byteWidth,
            final int scale,
            @NotNull final BigInteger truncationMask,
            final byte @NotNull [] nullValue) throws IOException {
        if (value.scale() != scale) {
            value = value.setScale(scale, RoundingMode.HALF_UP);
        }

        byte[] bytes = value.unscaledValue().and(truncationMask).toByteArray();
        int numZeroBytes = byteWidth - bytes.length;
        Assert.geqZero(numZeroBytes, "numZeroBytes");
        if (numZeroBytes > 0) {
            output.write(nullValue, 0, numZeroBytes);
        }
        output.write(bytes);
    }

    private static ChunkWriter<ByteChunk<Values>> intFromByte(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();

        switch (bitWidth) {
            case 8:
                return ByteChunkWriter.getIdentity(typeInfo.arrowField().isNullable());
            case 16:
                return new ShortChunkWriter<>(ByteChunk::isNull, ByteChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.shortCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>(ByteChunk::isNull, ByteChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>(ByteChunk::isNull, ByteChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.longCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<ShortChunk<Values>> intFromShort(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>(ShortChunk::isNull, ShortChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 16:
                return ShortChunkWriter.getIdentity(typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>(ShortChunk::isNull, ShortChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>(ShortChunk::isNull, ShortChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.longCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<IntChunk<Values>> intFromInt(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>(IntChunk::isNull, IntChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 16:
                return new ShortChunkWriter<>(IntChunk::isNull, IntChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.shortCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 32:
                return IntChunkWriter.getIdentity(typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>(IntChunk::isNull, IntChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.longCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<LongChunk<Values>> intFromLong(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>(LongChunk::isNull, LongChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 16:
                return new ShortChunkWriter<>(LongChunk::isNull, LongChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.shortCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>(LongChunk::isNull, LongChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 64:
                return LongChunkWriter.getIdentity(typeInfo.arrowField().isNullable());
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<ObjectChunk<BigInteger, Values>> intFromObject(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 16:
                return new ShortChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.shortCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.longCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<CharChunk<Values>> intFromChar(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>(CharChunk::isNull, CharChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 16:
                if (unsigned) {
                    return CharChunkWriter.getIdentity(typeInfo.arrowField().isNullable());
                } else {
                    return new ShortChunkWriter<>(CharChunk::isNull, CharChunk::getEmptyChunk,
                            (chunk, ii) -> QueryLanguageFunctionUtils.shortCast(chunk.get(ii)),
                            typeInfo.arrowField().isNullable());
                }
            case 32:
                return new IntChunkWriter<>(CharChunk::isNull, CharChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>(CharChunk::isNull, CharChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.longCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<FloatChunk<Values>> intFromFloat(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>(FloatChunk::isNull, FloatChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 16:
                return new ShortChunkWriter<>(FloatChunk::isNull, FloatChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.shortCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>(FloatChunk::isNull, FloatChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>(FloatChunk::isNull, FloatChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.longCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<DoubleChunk<Values>> intFromDouble(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>(DoubleChunk::isNull, DoubleChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.byteCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 16:
                return new ShortChunkWriter<>(DoubleChunk::isNull, DoubleChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.shortCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>(DoubleChunk::isNull, DoubleChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>(DoubleChunk::isNull, DoubleChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.longCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());
            default:
                throw new IllegalArgumentException("Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<ByteChunk<Values>> boolFromBoolean(
            final BarrageTypeInfo<Field> typeInfo) {
        return BooleanChunkWriter.getIdentity(typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ObjectChunk<byte[], Values>> fixedSizeBinaryFromByteArray(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FixedSizeBinary fixedSizeBinary = (ArrowType.FixedSizeBinary) typeInfo.arrowField().getType();
        final int elementWidth = fixedSizeBinary.getByteWidth();
        return new FixedWidthChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, elementWidth, false,
                typeInfo.arrowField().isNullable(),
                (out, chunk, offset) -> {
                    final byte[] data = chunk.get(offset);
                    if (data.length != elementWidth) {
                        throw new IllegalArgumentException(String.format(
                                "Expected fixed size binary of %d bytes, but got %d bytes when serializing %s",
                                elementWidth, data.length, typeInfo.type().getCanonicalName()));
                    }
                    out.write(data);
                });
    }

    private static ChunkWriter<IntChunk<Values>> dateFromInt(
            final BarrageTypeInfo<Field> typeInfo) {
        // see dateFromLocalDate's comment for more information on wire format
        final ArrowType.Date dateType = (ArrowType.Date) typeInfo.arrowField().getType();
        switch (dateType.getUnit()) {
            case DAY:
                return new IntChunkWriter<>(IntChunk::isNull, IntChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());

            case MILLISECOND:
                final long factor = Duration.ofDays(1).toMillis();
                return new LongChunkWriter<>(IntChunk::isNull, IntChunk::getEmptyChunk, (chunk, ii) -> {
                    final long value = QueryLanguageFunctionUtils.longCast(chunk.get(ii));
                    return value == QueryConstants.NULL_LONG
                            ? QueryConstants.NULL_LONG
                            : (value * factor);
                }, typeInfo.arrowField().isNullable());
            default:
                throw new IllegalArgumentException("Unexpected date unit: " + dateType.getUnit());
        }
    }

    private static ChunkWriter<LongChunk<Values>> dateFromLong(
            final BarrageTypeInfo<Field> typeInfo) {
        // see dateFromLocalDate's comment for more information on wire format
        final ArrowType.Date dateType = (ArrowType.Date) typeInfo.arrowField().getType();
        switch (dateType.getUnit()) {
            case DAY:
                return new IntChunkWriter<>(LongChunk::isNull, LongChunk::getEmptyChunk,
                        (chunk, ii) -> QueryLanguageFunctionUtils.intCast(chunk.get(ii)),
                        typeInfo.arrowField().isNullable());

            case MILLISECOND:
                final long factor = Duration.ofDays(1).toMillis();
                return new LongChunkWriter<>(LongChunk::isNull, LongChunk::getEmptyChunk, (chunk, ii) -> {
                    final long value = chunk.get(ii);
                    return value == QueryConstants.NULL_LONG
                            ? QueryConstants.NULL_LONG
                            : (value * factor);
                }, typeInfo.arrowField().isNullable());
            default:
                throw new IllegalArgumentException("Unexpected date unit: " + dateType.getUnit());
        }
    }

    private static ChunkWriter<ObjectChunk<LocalDate, Values>> dateFromLocalDate(
            final BarrageTypeInfo<Field> typeInfo) {
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

        final ArrowType.Date dateType = (ArrowType.Date) typeInfo.arrowField().getType();
        switch (dateType.getUnit()) {
            case DAY:
                return new IntChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, (chunk, ii) -> {
                    final LocalDate value = chunk.get(ii);
                    return value == null ? QueryConstants.NULL_INT : (int) value.toEpochDay();
                }, typeInfo.arrowField().isNullable());
            case MILLISECOND:
                final long factor = Duration.ofDays(1).toMillis();
                return new LongChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, (chunk, ii) -> {
                    final LocalDate value = chunk.get(ii);
                    return value == null ? QueryConstants.NULL_LONG : value.toEpochDay() * factor;
                }, typeInfo.arrowField().isNullable());
            default:
                throw new IllegalArgumentException("Unexpected date unit: " + dateType.getUnit());
        }
    }

    private static ChunkWriter<LongChunk<Values>> intervalFromDurationLong(
            final BarrageTypeInfo<Field> typeInfo) {
        // See intervalFromPeriod's comment for more information on wire format.

        final ArrowType.Interval intervalType = (ArrowType.Interval) typeInfo.arrowField().getType();
        switch (intervalType.getUnit()) {
            case YEAR_MONTH:
            case MONTH_DAY_NANO:
                throw new IllegalArgumentException(String.format(
                        "Do not support %s interval from duration as long conversion", intervalType));

            case DAY_TIME:
                final long nsPerDay = Duration.ofDays(1).toNanos();
                final long nsPerMs = Duration.ofMillis(1).toNanos();
                return new FixedWidthChunkWriter<>(LongChunk::isNull, LongChunk::getEmptyChunk, Integer.BYTES * 2,
                        false, typeInfo.arrowField().isNullable(),
                        (out, source, offset) -> {
                            final long value = source.get(offset);
                            if (value == QueryConstants.NULL_LONG) {
                                out.writeInt(0);
                                out.writeInt(0);
                            } else {
                                // days then millis
                                out.writeInt((int) (value / nsPerDay));
                                out.writeInt((int) ((value % nsPerDay) / nsPerMs));
                            }
                        });

            default:
                throw new IllegalArgumentException("Unexpected interval unit: " + intervalType.getUnit());
        }
    }

    private static ChunkWriter<ObjectChunk<Duration, Values>> intervalFromDuration(
            final BarrageTypeInfo<Field> typeInfo) {
        // See intervalFromPeriod's comment for more information on wire format.

        final ArrowType.Interval intervalType = (ArrowType.Interval) typeInfo.arrowField().getType();
        switch (intervalType.getUnit()) {
            case YEAR_MONTH:
            case MONTH_DAY_NANO:
                throw new IllegalArgumentException(String.format(
                        "Do not support %s interval from duration as long conversion", intervalType));

            case DAY_TIME:
                final long nsPerMs = Duration.ofMillis(1).toNanos();
                return new FixedWidthChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, Integer.BYTES * 2,
                        false, typeInfo.arrowField().isNullable(),
                        (out, source, offset) -> {
                            final Duration value = source.get(offset);
                            if (value == null) {
                                out.writeInt(0);
                                out.writeInt(0);
                            } else {
                                // days then millis
                                out.writeInt((int) value.toDays());
                                out.writeInt((int) (value.getNano() / nsPerMs));
                            }
                        });

            default:
                throw new IllegalArgumentException("Unexpected interval unit: " + intervalType.getUnit());
        }
    }

    private static ChunkWriter<ObjectChunk<Period, Values>> intervalFromPeriod(
            final BarrageTypeInfo<Field> typeInfo) {
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
        final ArrowType.Interval intervalType = (ArrowType.Interval) typeInfo.arrowField().getType();
        switch (intervalType.getUnit()) {
            case YEAR_MONTH:
                return new IntChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, (chunk, ii) -> {
                    final Period value = chunk.get(ii);
                    return value == null ? QueryConstants.NULL_INT : value.getMonths() + value.getYears() * 12;
                }, typeInfo.arrowField().isNullable());
            case DAY_TIME:
                return new FixedWidthChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, Integer.BYTES * 2,
                        false, typeInfo.arrowField().isNullable(),
                        (out, chunk, offset) -> {
                            final Period value = chunk.get(offset);
                            if (value == null) {
                                out.writeInt(0);
                                out.writeInt(0);
                            } else {
                                // days then millis
                                out.writeInt(value.getDays());
                                out.writeInt(0);
                            }
                        });
            case MONTH_DAY_NANO:
                return new FixedWidthChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                        Integer.BYTES * 2 + Long.BYTES, false, typeInfo.arrowField().isNullable(),
                        (out, chunk, offset) -> {
                            final Period value = chunk.get(offset);
                            if (value == null) {
                                out.writeInt(0);
                                out.writeInt(0);
                                out.writeLong(0);
                            } else {
                                out.writeInt(value.getMonths() + value.getYears() * 12);
                                out.writeInt(value.getDays());
                                out.writeLong(0);
                            }
                        });
            default:
                throw new IllegalArgumentException("Unexpected interval unit: " + intervalType.getUnit());
        }
    }

    private static ChunkWriter<ObjectChunk<PeriodDuration, Values>> intervalFromPeriodDuration(
            final BarrageTypeInfo<Field> typeInfo) {
        // See intervalToPeriod's comment for more information on wire format.

        final ArrowType.Interval intervalType = (ArrowType.Interval) typeInfo.arrowField().getType();
        switch (intervalType.getUnit()) {
            case YEAR_MONTH:
                return new IntChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, (chunk, ii) -> {
                    final Period value = chunk.get(ii).getPeriod();
                    return value == null ? QueryConstants.NULL_INT : value.getMonths() + value.getYears() * 12;
                }, typeInfo.arrowField().isNullable());
            case DAY_TIME:
                return new FixedWidthChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, Integer.BYTES * 2,
                        false, typeInfo.arrowField().isNullable(),
                        (out, chunk, offset) -> {
                            final PeriodDuration value = chunk.get(offset);
                            if (value == null) {
                                out.writeInt(0);
                                out.writeInt(0);
                            } else {
                                // days then millis
                                out.writeInt(value.getPeriod().getDays());
                                out.writeInt(value.getDuration().getNano());
                            }
                        });
            case MONTH_DAY_NANO:
                return new FixedWidthChunkWriter<>(ObjectChunk::isNull, ObjectChunk::getEmptyChunk,
                        Integer.BYTES * 2 + Long.BYTES, false, typeInfo.arrowField().isNullable(),
                        (out, chunk, offset) -> {
                            final PeriodDuration value = chunk.get(offset);
                            if (value == null) {
                                out.writeInt(0);
                                out.writeInt(0);
                                out.writeLong(0);
                            } else {
                                final Period period = value.getPeriod();
                                out.writeInt(period.getMonths() + period.getYears() * 12);
                                out.writeInt(period.getDays());
                                out.writeLong(value.getDuration().getNano());
                            }
                        });
            default:
                throw new IllegalArgumentException("Unexpected interval unit: " + intervalType.getUnit());
        }
    }
}
