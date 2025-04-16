//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.rpc.Code;
import io.deephaven.UncheckedDeephavenException;
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
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
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
import io.deephaven.proto.util.Exceptions;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.Vector;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.deephaven.extensions.barrage.chunk.FactoryHelper.factorForTimeUnit;
import static io.deephaven.extensions.barrage.chunk.FactoryHelper.maskIfOverflow;

/**
 * JVM implementation of {@link ChunkWriter.Factory}, suitable for use in Java clients and servers. This default
 * implementation may not round trip flight types in a stable way, but will round trip Deephaven table definitions and
 * table data. Neither of these is a required/expected property of being a Flight/Barrage/Deephaven client.
 */
public class DefaultChunkWriterFactory implements ChunkWriter.Factory {
    public static final Logger log = LoggerFactory.getLogger(DefaultChunkWriterFactory.class);
    public static final DefaultChunkWriterFactory INSTANCE = new DefaultChunkWriterFactory();

    /**
     * This supplier interface simplifies the cost to operate off of the ArrowType directly since the Arrow POJO is not
     * yet supported over GWT.
     */
    public interface ArrowTypeChunkWriterSupplier {
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
        register(ArrowType.ArrowTypeID.Timestamp, LocalDateTime.class,
                DefaultChunkWriterFactory::timestampFromLocalDateTime);
        register(ArrowType.ArrowTypeID.Utf8, String.class, DefaultChunkWriterFactory::utf8FromObject);
        register(ArrowType.ArrowTypeID.Duration, Duration.class, DefaultChunkWriterFactory::durationFromDuration);
        register(ArrowType.ArrowTypeID.FloatingPoint, byte.class, DefaultChunkWriterFactory::fpFromByte);
        register(ArrowType.ArrowTypeID.FloatingPoint, char.class, DefaultChunkWriterFactory::fpFromChar);
        register(ArrowType.ArrowTypeID.FloatingPoint, short.class, DefaultChunkWriterFactory::fpFromShort);
        register(ArrowType.ArrowTypeID.FloatingPoint, int.class, DefaultChunkWriterFactory::fpFromInt);
        register(ArrowType.ArrowTypeID.FloatingPoint, long.class, DefaultChunkWriterFactory::fpFromLong);
        register(ArrowType.ArrowTypeID.FloatingPoint, BigInteger.class, DefaultChunkWriterFactory::fpFromBigInteger);
        register(ArrowType.ArrowTypeID.FloatingPoint, float.class, DefaultChunkWriterFactory::fpFromFloat);
        register(ArrowType.ArrowTypeID.FloatingPoint, double.class, DefaultChunkWriterFactory::fpFromDouble);
        register(ArrowType.ArrowTypeID.FloatingPoint, BigDecimal.class, DefaultChunkWriterFactory::fpFromBigDecimal);
        register(ArrowType.ArrowTypeID.Binary, byte[].class, DefaultChunkWriterFactory::binaryFromByteArray);
        register(ArrowType.ArrowTypeID.Binary, ByteVector.class, DefaultChunkWriterFactory::binaryFromByteVector);
        register(ArrowType.ArrowTypeID.Binary, ByteBuffer.class, DefaultChunkWriterFactory::binaryFromByteBuffer);
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
        // note that we hold boolean's in ByteChunks, so it's identical logic to read boolean as bytes.
        register(ArrowType.ArrowTypeID.Bool, byte.class, DefaultChunkWriterFactory::boolFromBoolean);
        register(ArrowType.ArrowTypeID.FixedSizeBinary, byte[].class,
                DefaultChunkWriterFactory::fixedSizeBinaryFromByteArray);
        register(ArrowType.ArrowTypeID.FixedSizeBinary, ByteVector.class,
                DefaultChunkWriterFactory::fixedSizeBinaryFromByteVector);
        register(ArrowType.ArrowTypeID.FixedSizeBinary, ByteBuffer.class,
                DefaultChunkWriterFactory::fixedSizeBinaryFromByteBuffer);
        register(ArrowType.ArrowTypeID.Date, LocalDate.class, DefaultChunkWriterFactory::dateFromLocalDate);
        register(ArrowType.ArrowTypeID.Interval, Duration.class, DefaultChunkWriterFactory::intervalFromDuration);
        register(ArrowType.ArrowTypeID.Interval, Period.class, DefaultChunkWriterFactory::intervalFromPeriod);
        register(ArrowType.ArrowTypeID.Interval, PeriodDuration.class,
                DefaultChunkWriterFactory::intervalFromPeriodDuration);

        // These are DH custom wire formats
        register(ArrowType.ArrowTypeID.Utf8, ArrayPreview.class, DefaultChunkWriterFactory::utf8FromObject);
        register(ArrowType.ArrowTypeID.Utf8, DisplayWrapper.class, DefaultChunkWriterFactory::utf8FromObject);
        register(ArrowType.ArrowTypeID.Binary, String.class, DefaultChunkWriterFactory::utf8FromObject);
        register(ArrowType.ArrowTypeID.Binary, BigInteger.class, DefaultChunkWriterFactory::binaryFromBigInt);
        register(ArrowType.ArrowTypeID.Binary, BigDecimal.class, DefaultChunkWriterFactory::binaryFromBigDecimal);
        register(ArrowType.ArrowTypeID.Binary, Schema.class, DefaultChunkWriterFactory::binaryFromSchema);
    }

    /**
     * Disables the default behavior of converting unknown types to their {@code toString()} representation.
     * <p>
     * By default, the {@code DefaultChunkWriterFactory} will use an encoder that invokes {@code toString()} on any
     * incoming types it does not recognize or have a specific handler for. This method disables that behavior, ensuring
     * that unsupported types throw an exception when a writer cannot be provided.
     */
    public void disableToStringUnknownTypes() {
        toStringUnknownTypes = false;
    }

    protected Map<Class<?>, DefaultChunkWriterFactory.ArrowTypeChunkWriterSupplier> lookupWriterFactory(
            final ArrowType.ArrowTypeID typeId) {
        return registeredFactories.get(typeId);
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

        // Note we do not support these as they require 64-bit offset support:
        if (typeId == ArrowType.ArrowTypeID.LargeUtf8
                || typeId == ArrowType.ArrowTypeID.LargeBinary
                || typeId == ArrowType.ArrowTypeID.LargeList) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                    "No support for 64-bit offsets to map arrow type %s from %s.",
                    field.getType().toString(),
                    typeInfo.type().getCanonicalName()));
        }

        final Map<Class<?>, ArrowTypeChunkWriterSupplier> knownWriters = lookupWriterFactory(typeId);
        if (knownWriters == null && !isSpecialType) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                    "No known Barrage ChunkWriter for arrow type %s from %s.",
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
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                    "No known Barrage ChunkWriter for arrow type %s from %s. \nSupported types: \n\t%s",
                    field.getType().toString(),
                    typeInfo.type().getCanonicalName(),
                    knownWriters.keySet().stream().map(Object::toString).collect(Collectors.joining(",\n\t"))));
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
                Class<?> componentType =
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
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                        "No known Barrage ChunkWriter for arrow type %s from %s. Expected destination type to be an array.",
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
            final Field structField = field.getChildren().get(0);
            final BarrageTypeInfo<Field> keyTypeInfo = BarrageUtil.getDefaultType(structField.getChildren().get(0));
            final BarrageTypeInfo<Field> valueTypeInfo = BarrageUtil.getDefaultType(structField.getChildren().get(1));

            final ChunkWriter<Chunk<Values>> keyWriter = newWriterPojo(keyTypeInfo);
            final ChunkWriter<Chunk<Values>> valueWriter = newWriterPojo(valueTypeInfo);

            // noinspection unchecked
            return (ChunkWriter<T>) new MapChunkWriter<>(
                    keyWriter, valueWriter, keyTypeInfo.chunkType(), valueTypeInfo.chunkType(), field.isNullable());
        }

        // TODO (DH-18679): struct support
        // expose @FunctionalInterface of Map<String, Chunk<Values>> -> T?
        // if (typeId == ArrowType.ArrowTypeID.Struct) {

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
                    childChunkTypes, unionType.getTypeIds());
        }

        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                "No known ChunkWriter for arrow type %s from %s. \nArrow types supported: \n\t%s",
                field.getType().toString(),
                typeInfo.type().getCanonicalName(),
                knownWriters == null ? "none"
                        : knownWriters.keySet().stream()
                                .map(Object::toString)
                                .collect(Collectors.joining(",\n\t"))));
    }

    @SuppressWarnings("unchecked")
    public void register(
            final ArrowType.ArrowTypeID arrowType,
            final Class<?> deephavenType,
            final ArrowTypeChunkWriterSupplier chunkWriterFactory) {
        registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                .put(deephavenType, chunkWriterFactory);

        // if primitive automatically register the boxed version of this mapping, too
        if (deephavenType == byte.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Byte.class, typeInfo -> ByteChunkWriter.makeBoxed(
                            (ChunkWriter<ByteChunk<Values>>) chunkWriterFactory.make(typeInfo)));
        } else if (deephavenType == short.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Short.class, typeInfo -> ShortChunkWriter.makeBoxed(
                            (ChunkWriter<ShortChunk<Values>>) chunkWriterFactory.make(typeInfo)));
        } else if (deephavenType == int.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Integer.class, typeInfo -> IntChunkWriter.makeBoxed(
                            (ChunkWriter<IntChunk<Values>>) chunkWriterFactory.make(typeInfo)));
        } else if (deephavenType == long.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Long.class, typeInfo -> LongChunkWriter.makeBoxed(
                            (ChunkWriter<LongChunk<Values>>) chunkWriterFactory.make(typeInfo)));
        } else if (deephavenType == char.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Character.class, typeInfo -> CharChunkWriter.makeBoxed(
                            (ChunkWriter<CharChunk<Values>>) chunkWriterFactory.make(typeInfo)));
        } else if (deephavenType == float.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Float.class, typeInfo -> FloatChunkWriter.makeBoxed(
                            (ChunkWriter<FloatChunk<Values>>) chunkWriterFactory.make(typeInfo)));
        } else if (deephavenType == double.class) {
            registeredFactories.computeIfAbsent(arrowType, k -> new HashMap<>())
                    .put(Double.class, typeInfo -> DoubleChunkWriter.makeBoxed(
                            (ChunkWriter<DoubleChunk<Values>>) chunkWriterFactory.make(typeInfo)));
        }
    }

    private static ChunkWriter<Chunk<Values>> timestampFromZonedDateTime(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Timestamp tsType = (ArrowType.Timestamp) typeInfo.arrowField().getType();
        final long factor = factorForTimeUnit(tsType.getUnit());
        // TODO (https://github.com/deephaven/deephaven-core/issues/5241): Inconsistent handling of ZonedDateTime
        // we do not know whether the incoming chunk source is a LongChunk or ObjectChunk<ZonedDateTime>
        return new LongChunkWriter<>((Chunk<Values> source) -> {
            if (source instanceof LongChunk && factor == 1) {
                return source;
            }

            final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
            if (source instanceof LongChunk) {
                final LongChunk<Values> longChunk = source.asLongChunk();
                for (int ii = 0; ii < source.size(); ++ii) {
                    final long value = longChunk.get(ii);
                    chunk.set(ii, longChunk.isNull(ii) ? value : value / factor);
                }
            } else {
                final ObjectChunk<ZonedDateTime, Values> zdtChunk = source.asObjectChunk();
                for (int ii = 0; ii < source.size(); ++ii) {
                    final ZonedDateTime value = zdtChunk.get(ii);
                    chunk.set(ii, value == null
                            ? QueryConstants.NULL_LONG
                            : DateTimeUtils.epochNanos(value) / factor);
                }
            }
            return chunk;
        }, LongChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ObjectChunk<LocalDateTime, Values>> timestampFromLocalDateTime(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Timestamp tsType = (ArrowType.Timestamp) typeInfo.arrowField().getType();
        final long factor = factorForTimeUnit(tsType.getUnit());
        final String timezone = tsType.getTimezone();
        final ZoneId tz = timezone == null ? ZoneId.of("UTC") : DateTimeUtils.parseTimeZone(timezone);
        return new LongChunkWriter<>((ObjectChunk<LocalDateTime, Values> source) -> {
            final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
            for (int ii = 0; ii < source.size(); ++ii) {
                final LocalDateTime value = source.get(ii);
                if (value == null) {
                    chunk.set(ii, QueryConstants.NULL_LONG);
                } else {
                    final ZonedDateTime zdt = value.atZone(tz).withZoneSameInstant(ZoneId.of("UTC"));
                    chunk.set(ii, DateTimeUtils.epochNanos(zdt) / factor);
                }
            }
            return chunk;
        }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<Chunk<Values>> timestampFromLong(
            final BarrageTypeInfo<Field> typeInfo) {
        final long factor = factorForTimeUnit(((ArrowType.Timestamp) typeInfo.arrowField().getType()).getUnit());
        return new LongChunkWriter<>((Chunk<Values> source) -> {
            final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
            if (source instanceof LongChunk) {
                final LongChunk<Values> longSource = source.asLongChunk();
                for (int ii = 0; ii < source.size(); ++ii) {
                    final long value = longSource.get(ii);
                    chunk.set(ii, value == QueryConstants.NULL_LONG
                            ? QueryConstants.NULL_LONG
                            : value / factor);
                }
            } else if (source instanceof ObjectChunk) {
                final ObjectChunk<ZonedDateTime, Values> zdtSource = source.asObjectChunk();
                for (int ii = 0; ii < source.size(); ++ii) {
                    final ZonedDateTime value = zdtSource.get(ii);
                    chunk.set(ii, value == null
                            ? QueryConstants.NULL_LONG
                            : DateTimeUtils.epochNanos(value) / factor);
                }
            } else {
                throw new UncheckedDeephavenException("Unexpected chunk type: " + source.getClass());
            }
            return chunk;
        }, LongChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ObjectChunk<Instant, Values>> timestampFromInstant(
            final BarrageTypeInfo<Field> typeInfo) {
        final long factor = factorForTimeUnit(((ArrowType.Timestamp) typeInfo.arrowField().getType()).getUnit());
        return new LongChunkWriter<>((ObjectChunk<Instant, Values> source) -> {
            final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
            for (int ii = 0; ii < source.size(); ++ii) {
                final Instant value = source.get(ii);
                chunk.set(ii, value == null
                        ? QueryConstants.NULL_LONG
                        : (value.getEpochSecond() * 1_000_000_000L + value.getNano()) / factor);
            }
            return chunk;
        }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ObjectChunk<Object, Values>> utf8FromObject(
            final BarrageTypeInfo<Field> typeInfo) {
        return new VarBinaryChunkWriter<>(typeInfo.arrowField().isNullable(),
                (out, item) -> out.write(item.toString().getBytes(StandardCharsets.UTF_8)));
    }

    private static ChunkWriter<ObjectChunk<Duration, Values>> durationFromDuration(
            final BarrageTypeInfo<Field> typeInfo) {
        final long factor = factorForTimeUnit(((ArrowType.Duration) typeInfo.arrowField().getType()).getUnit());
        return new LongChunkWriter<>((ObjectChunk<Duration, Values> source) -> {
            final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
            for (int ii = 0; ii < source.size(); ++ii) {
                final Duration value = source.get(ii);
                chunk.set(ii, value == null ? QueryConstants.NULL_LONG : value.toNanos() / factor);
            }
            return chunk;
        }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ByteChunk<Values>> fpFromByte(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>((ByteChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final byte value = source.get(ii);
                        chunk.set(ii, value == QueryConstants.NULL_BYTE
                                ? QueryConstants.NULL_SHORT
                                : Float16.toFloat16(value));
                    }
                    return chunk;
                }, ByteChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case SINGLE:
                return new FloatChunkWriter<>((ByteChunk<Values> source) -> {
                    final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.floatCast(source.get(ii)));
                    }
                    return chunk;
                }, ByteChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case DOUBLE:
                return new DoubleChunkWriter<>((ByteChunk<Values> source) -> {
                    final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.doubleCast(source.get(ii)));
                    }
                    return chunk;
                }, ByteChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<CharChunk<Values>> fpFromChar(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>((CharChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final char value = source.get(ii);
                        chunk.set(ii, value == QueryConstants.NULL_CHAR
                                ? QueryConstants.NULL_SHORT
                                : Float16.toFloat16(value));
                    }
                    return chunk;
                }, CharChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case SINGLE:
                return new FloatChunkWriter<>((CharChunk<Values> source) -> {
                    final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.floatCast(source.get(ii)));
                    }
                    return chunk;
                }, CharChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case DOUBLE:
                return new DoubleChunkWriter<>((CharChunk<Values> source) -> {
                    final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.doubleCast(source.get(ii)));
                    }
                    return chunk;
                }, CharChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<ShortChunk<Values>> fpFromShort(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>((ShortChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final short value = source.get(ii);
                        chunk.set(ii, value == QueryConstants.NULL_SHORT
                                ? QueryConstants.NULL_SHORT
                                : Float16.toFloat16(value));
                    }
                    return chunk;
                }, ShortChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case SINGLE:
                return new FloatChunkWriter<>((ShortChunk<Values> source) -> {
                    final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.floatCast(source.get(ii)));
                    }
                    return chunk;
                }, ShortChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case DOUBLE:
                return new DoubleChunkWriter<>((ShortChunk<Values> source) -> {
                    final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.doubleCast(source.get(ii)));
                    }
                    return chunk;
                }, ShortChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<IntChunk<Values>> fpFromInt(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>((IntChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final int value = source.get(ii);
                        chunk.set(ii, value == QueryConstants.NULL_INT
                                ? QueryConstants.NULL_SHORT
                                : Float16.toFloat16(value));
                    }
                    return chunk;
                }, IntChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case SINGLE:
                return new FloatChunkWriter<>((IntChunk<Values> source) -> {
                    final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.floatCast(source.get(ii)));
                    }
                    return chunk;
                }, IntChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case DOUBLE:
                return new DoubleChunkWriter<>((IntChunk<Values> source) -> {
                    final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.doubleCast(source.get(ii)));
                    }
                    return chunk;
                }, IntChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<LongChunk<Values>> fpFromLong(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>((LongChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final long value = source.get(ii);
                        chunk.set(ii, value == QueryConstants.NULL_LONG
                                ? QueryConstants.NULL_SHORT
                                : Float16.toFloat16(value));
                    }
                    return chunk;
                }, LongChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case SINGLE:
                return new FloatChunkWriter<>((LongChunk<Values> source) -> {
                    final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.floatCast(source.get(ii)));
                    }
                    return chunk;
                }, LongChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case DOUBLE:
                return new DoubleChunkWriter<>((LongChunk<Values> source) -> {
                    final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.doubleCast(source.get(ii)));
                    }
                    return chunk;
                }, LongChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<ObjectChunk<BigInteger, Values>> fpFromBigInteger(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>((ObjectChunk<BigInteger, Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final BigInteger value = source.get(ii);
                        chunk.set(ii, value == null
                                ? QueryConstants.NULL_SHORT
                                : Float16.toFloat16(value.floatValue()));
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case SINGLE:
                return new FloatChunkWriter<>((ObjectChunk<BigInteger, Values> source) -> {
                    final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.floatCast(source.get(ii)));
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case DOUBLE:
                return new DoubleChunkWriter<>((ObjectChunk<BigInteger, Values> source) -> {
                    final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.doubleCast(source.get(ii)));
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<FloatChunk<Values>> fpFromFloat(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>((FloatChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final float value = source.get(ii);
                        chunk.set(ii, value == QueryConstants.NULL_FLOAT
                                ? QueryConstants.NULL_SHORT
                                : Float16.toFloat16(value));
                    }
                    return chunk;
                }, FloatChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case SINGLE:
                return FloatChunkWriter.getIdentity(typeInfo.arrowField().isNullable());

            case DOUBLE:
                return new DoubleChunkWriter<>((FloatChunk<Values> source) -> {
                    final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.doubleCast(source.get(ii)));
                    }
                    return chunk;
                }, FloatChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<DoubleChunk<Values>> fpFromDouble(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>((DoubleChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final double value = source.get(ii);
                        chunk.set(ii, value == QueryConstants.NULL_DOUBLE
                                ? QueryConstants.NULL_SHORT
                                : Float16.toFloat16((float) value));
                    }
                    return chunk;
                }, DoubleChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case SINGLE:
                return new FloatChunkWriter<>((DoubleChunk<Values> source) -> {
                    final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.floatCast(source.get(ii)));
                    }
                    return chunk;
                }, DoubleChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case DOUBLE:
                return DoubleChunkWriter.getIdentity(typeInfo.arrowField().isNullable());

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<ObjectChunk<BigDecimal, Values>> fpFromBigDecimal(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) typeInfo.arrowField().getType();
        switch (fpType.getPrecision()) {
            case HALF:
                return new ShortChunkWriter<>((ObjectChunk<BigDecimal, Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final BigDecimal value = source.get(ii);
                        chunk.set(ii, value == null
                                ? QueryConstants.NULL_SHORT
                                : Float16.toFloat16(value.floatValue()));
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case SINGLE:
                return new FloatChunkWriter<>((ObjectChunk<BigDecimal, Values> source) -> {
                    final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.floatCast(source.get(ii)));
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case DOUBLE:
                return new DoubleChunkWriter<>((ObjectChunk<BigDecimal, Values> source) -> {
                    final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.doubleCast(source.get(ii)));
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected floating point precision: " + fpType.getPrecision());
        }
    }

    private static ChunkWriter<ObjectChunk<byte[], Values>> binaryFromByteArray(
            final BarrageTypeInfo<Field> typeInfo) {
        return new VarBinaryChunkWriter<>(typeInfo.arrowField().isNullable(),
                OutputStream::write);
    }

    private static ChunkWriter<ObjectChunk<ByteVector, Values>> binaryFromByteVector(
            final BarrageTypeInfo<Field> typeInfo) {
        return new VarBinaryChunkWriter<>(typeInfo.arrowField().isNullable(), (out, bv) -> out.write(bv.toArray()));
    }

    private static ChunkWriter<ObjectChunk<ByteBuffer, Values>> binaryFromByteBuffer(
            final BarrageTypeInfo<Field> typeInfo) {
        return new VarBinaryChunkWriter<>(typeInfo.arrowField().isNullable(), (out, bb) -> out.write(bb.array()));
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
                return new IntChunkWriter<>((ObjectChunk<LocalTime, Values> source) -> {
                    final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final LocalTime value = source.get(ii);
                        chunk.set(ii, value == null ? QueryConstants.NULL_INT : (int) (value.toNanoOfDay() / factor));
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case 64:
                return new LongChunkWriter<>((ObjectChunk<LocalTime, Values> source) -> {
                    final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final LocalTime value = source.get(ii);
                        chunk.set(ii, value == null ? QueryConstants.NULL_LONG : value.toNanoOfDay() / factor);
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<ByteChunk<Values>> decimalFromByte(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;

        return new BigDecimalChunkWriter<>((ByteChunk<Values> source) -> {
            final WritableObjectChunk<BigDecimal, Values> chunk = WritableObjectChunk.makeWritableChunk(source.size());
            for (int ii = 0; ii < source.size(); ++ii) {
                final byte value = source.get(ii);
                if (value == QueryConstants.NULL_BYTE) {
                    chunk.set(ii, null);
                    continue;
                }

                chunk.set(ii, BigDecimal.valueOf(value));
            }
            return chunk;
        }, decimalType, ByteChunk::getEmptyChunk, byteWidth, false, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<CharChunk<Values>> decimalFromChar(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;

        return new BigDecimalChunkWriter<>((CharChunk<Values> source) -> {
            final WritableObjectChunk<BigDecimal, Values> chunk = WritableObjectChunk.makeWritableChunk(source.size());
            for (int ii = 0; ii < source.size(); ++ii) {
                final char value = source.get(ii);
                if (value == QueryConstants.NULL_CHAR) {
                    chunk.set(ii, null);
                    continue;
                }

                chunk.set(ii, BigDecimal.valueOf(value));
            }
            return chunk;
        }, decimalType, CharChunk::getEmptyChunk, byteWidth, false, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ShortChunk<Values>> decimalFromShort(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;

        return new BigDecimalChunkWriter<>((ShortChunk<Values> source) -> {
            final WritableObjectChunk<BigDecimal, Values> chunk = WritableObjectChunk.makeWritableChunk(source.size());
            for (int ii = 0; ii < source.size(); ++ii) {
                final short value = source.get(ii);
                if (value == QueryConstants.NULL_SHORT) {
                    chunk.set(ii, null);
                    continue;
                }

                chunk.set(ii, BigDecimal.valueOf(value));
            }
            return chunk;
        }, decimalType, ShortChunk::getEmptyChunk, byteWidth, false, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<IntChunk<Values>> decimalFromInt(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;

        return new BigDecimalChunkWriter<>((IntChunk<Values> source) -> {
            final WritableObjectChunk<BigDecimal, Values> chunk = WritableObjectChunk.makeWritableChunk(source.size());
            for (int ii = 0; ii < source.size(); ++ii) {
                final int value = source.get(ii);
                if (value == QueryConstants.NULL_INT) {
                    chunk.set(ii, null);
                    continue;
                }

                chunk.set(ii, BigDecimal.valueOf(value));
            }
            return chunk;
        }, decimalType, IntChunk::getEmptyChunk, byteWidth, false, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<LongChunk<Values>> decimalFromLong(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;

        return new BigDecimalChunkWriter<>((LongChunk<Values> source) -> {
            final WritableObjectChunk<BigDecimal, Values> chunk = WritableObjectChunk.makeWritableChunk(source.size());
            for (int ii = 0; ii < source.size(); ++ii) {
                final long value = source.get(ii);
                if (value == QueryConstants.NULL_LONG) {
                    chunk.set(ii, null);
                    continue;
                }

                chunk.set(ii, BigDecimal.valueOf(value));
            }
            return chunk;
        }, decimalType, LongChunk::getEmptyChunk, byteWidth, false, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ObjectChunk<BigInteger, Values>> decimalFromBigInteger(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;

        return new BigDecimalChunkWriter<>((ObjectChunk<BigInteger, Values> source) -> {
            final WritableObjectChunk<BigDecimal, Values> chunk = WritableObjectChunk.makeWritableChunk(source.size());
            for (int ii = 0; ii < source.size(); ++ii) {
                final BigInteger value = source.get(ii);
                if (value == null) {
                    chunk.set(ii, null);
                    continue;
                }

                chunk.set(ii, new BigDecimal(value));
            }
            return chunk;
        }, decimalType, ObjectChunk::getEmptyChunk, byteWidth, false, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<FloatChunk<Values>> decimalFromFloat(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;

        return new BigDecimalChunkWriter<>((FloatChunk<Values> source) -> {
            final WritableObjectChunk<BigDecimal, Values> chunk = WritableObjectChunk.makeWritableChunk(source.size());
            for (int ii = 0; ii < source.size(); ++ii) {
                final float value = source.get(ii);
                if (value == QueryConstants.NULL_FLOAT) {
                    chunk.set(ii, null);
                    continue;
                }

                chunk.set(ii, BigDecimal.valueOf(value));
            }
            return chunk;
        }, decimalType, FloatChunk::getEmptyChunk, byteWidth, false, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<DoubleChunk<Values>> decimalFromDouble(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;

        return new BigDecimalChunkWriter<>((DoubleChunk<Values> source) -> {
            final WritableObjectChunk<BigDecimal, Values> chunk = WritableObjectChunk.makeWritableChunk(source.size());
            for (int ii = 0; ii < source.size(); ++ii) {
                final double value = source.get(ii);
                if (value == QueryConstants.NULL_DOUBLE) {
                    chunk.set(ii, null);
                    continue;
                }

                chunk.set(ii, BigDecimal.valueOf(value));
            }
            return chunk;
        }, decimalType, DoubleChunk::getEmptyChunk, byteWidth, false, typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ObjectChunk<BigDecimal, Values>> decimalFromBigDecimal(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Decimal decimalType = (ArrowType.Decimal) typeInfo.arrowField().getType();
        final int byteWidth = decimalType.getBitWidth() / 8;

        return new BigDecimalChunkWriter<>(null, decimalType, ObjectChunk::getEmptyChunk, byteWidth, false,
                typeInfo.arrowField().isNullable());
    }

    private static ChunkWriter<ByteChunk<Values>> intFromByte(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return ByteChunkWriter.getIdentity(typeInfo.arrowField().isNullable());
            case 16:
                if (unsigned) {
                    return new CharChunkWriter<>((ByteChunk<Values> source) -> {
                        final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(source.size());
                        for (int ii = 0; ii < source.size(); ++ii) {
                            chunk.set(ii, QueryLanguageFunctionUtils.charCast(source.get(ii)));
                        }
                        return chunk;
                    }, ByteChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
                }
                return new ShortChunkWriter<>((ByteChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.shortCast(source.get(ii)));
                    }
                    return chunk;
                }, ByteChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>((ByteChunk<Values> source) -> {
                    final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, maskIfOverflow(unsigned, Byte.BYTES,
                                QueryLanguageFunctionUtils.intCast(source.get(ii))));
                    }
                    return chunk;
                }, ByteChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>((ByteChunk<Values> source) -> {
                    final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, maskIfOverflow(unsigned, Byte.BYTES,
                                QueryLanguageFunctionUtils.longCast(source.get(ii))));
                    }
                    return chunk;
                }, ByteChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<ShortChunk<Values>> intFromShort(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>((ShortChunk<Values> source) -> {
                    final WritableByteChunk<Values> chunk = WritableByteChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.byteCast(source.get(ii)));
                    }
                    return chunk;
                }, ShortChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 16:
                if (unsigned) {
                    return new CharChunkWriter<>((ShortChunk<Values> source) -> {
                        final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(source.size());
                        for (int ii = 0; ii < source.size(); ++ii) {
                            chunk.set(ii, QueryLanguageFunctionUtils.charCast(source.get(ii)));
                        }
                        return chunk;
                    }, ShortChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
                }
                return ShortChunkWriter.getIdentity(typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>((ShortChunk<Values> source) -> {
                    final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, maskIfOverflow(unsigned, Short.BYTES,
                                QueryLanguageFunctionUtils.intCast(source.get(ii))));
                    }
                    return chunk;
                }, ShortChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>((ShortChunk<Values> source) -> {
                    final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, maskIfOverflow(unsigned, Short.BYTES,
                                QueryLanguageFunctionUtils.longCast(source.get(ii))));
                    }
                    return chunk;
                }, ShortChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<IntChunk<Values>> intFromInt(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>((IntChunk<Values> source) -> {
                    final WritableByteChunk<Values> chunk = WritableByteChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.byteCast(source.get(ii)));
                    }
                    return chunk;
                }, IntChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 16:
                if (unsigned) {
                    return new CharChunkWriter<>((IntChunk<Values> source) -> {
                        final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(source.size());
                        for (int ii = 0; ii < source.size(); ++ii) {
                            chunk.set(ii, QueryLanguageFunctionUtils.charCast(source.get(ii)));
                        }
                        return chunk;
                    }, IntChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
                }
                return new ShortChunkWriter<>((IntChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.shortCast(source.get(ii)));
                    }
                    return chunk;
                }, IntChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 32:
                return IntChunkWriter.getIdentity(typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>((IntChunk<Values> source) -> {
                    final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, maskIfOverflow(unsigned, Integer.BYTES,
                                QueryLanguageFunctionUtils.longCast(source.get(ii))));
                    }
                    return chunk;
                }, IntChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<LongChunk<Values>> intFromLong(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>((LongChunk<Values> source) -> {
                    final WritableByteChunk<Values> chunk = WritableByteChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.byteCast(source.get(ii)));
                    }
                    return chunk;
                }, LongChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 16:
                if (unsigned) {
                    return new CharChunkWriter<>((LongChunk<Values> source) -> {
                        final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(source.size());
                        for (int ii = 0; ii < source.size(); ++ii) {
                            chunk.set(ii, QueryLanguageFunctionUtils.charCast(source.get(ii)));
                        }
                        return chunk;
                    }, LongChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
                }
                return new ShortChunkWriter<>((LongChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.shortCast(source.get(ii)));
                    }
                    return chunk;
                }, LongChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>((LongChunk<Values> source) -> {
                    final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.intCast(source.get(ii)));
                    }
                    return chunk;
                }, LongChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 64:
                return LongChunkWriter.getIdentity(typeInfo.arrowField().isNullable());
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<ObjectChunk<?, Values>> intFromObject(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>((ObjectChunk<?, Values> source) -> {
                    final WritableByteChunk<Values> chunk = WritableByteChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.byteCast(source.get(ii)));
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 16:
                return new ShortChunkWriter<>((ObjectChunk<?, Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.shortCast(source.get(ii)));
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>((ObjectChunk<?, Values> source) -> {
                    final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.intCast(source.get(ii)));
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>((ObjectChunk<?, Values> source) -> {
                    final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.longCast(source.get(ii)));
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<CharChunk<Values>> intFromChar(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>((CharChunk<Values> source) -> {
                    final WritableByteChunk<Values> chunk = WritableByteChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.byteCast(source.get(ii)));
                    }
                    return chunk;
                }, CharChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 16:
                if (unsigned) {
                    return CharChunkWriter.getIdentity(typeInfo.arrowField().isNullable());
                } else {
                    return new ShortChunkWriter<>((CharChunk<Values> source) -> {
                        final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                        for (int ii = 0; ii < source.size(); ++ii) {
                            chunk.set(ii, QueryLanguageFunctionUtils.shortCast(source.get(ii)));
                        }
                        return chunk;
                    }, CharChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
                }
            case 32:
                return new IntChunkWriter<>((CharChunk<Values> source) -> {
                    final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.intCast(source.get(ii)));
                    }
                    return chunk;
                }, CharChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>((CharChunk<Values> source) -> {
                    final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.longCast(source.get(ii)));
                    }
                    return chunk;
                }, CharChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<FloatChunk<Values>> intFromFloat(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>((FloatChunk<Values> source) -> {
                    final WritableByteChunk<Values> chunk = WritableByteChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.byteCast(source.get(ii)));
                    }
                    return chunk;
                }, FloatChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 16:
                if (unsigned) {
                    return new CharChunkWriter<>((FloatChunk<Values> source) -> {
                        final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(source.size());
                        for (int ii = 0; ii < source.size(); ++ii) {
                            chunk.set(ii, QueryLanguageFunctionUtils.charCast(source.get(ii)));
                        }
                        return chunk;
                    }, FloatChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
                }
                return new ShortChunkWriter<>((FloatChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.shortCast(source.get(ii)));
                    }
                    return chunk;
                }, FloatChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>((FloatChunk<Values> source) -> {
                    final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.intCast(source.get(ii)));
                    }
                    return chunk;
                }, FloatChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>((FloatChunk<Values> source) -> {
                    final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.longCast(source.get(ii)));
                    }
                    return chunk;
                }, FloatChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unexpected bit width: " + bitWidth);
        }
    }

    private static ChunkWriter<DoubleChunk<Values>> intFromDouble(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.Int intType = (ArrowType.Int) typeInfo.arrowField().getType();
        final int bitWidth = intType.getBitWidth();
        final boolean unsigned = !intType.getIsSigned();

        switch (bitWidth) {
            case 8:
                return new ByteChunkWriter<>((DoubleChunk<Values> source) -> {
                    final WritableByteChunk<Values> chunk = WritableByteChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.byteCast(source.get(ii)));
                    }
                    return chunk;
                }, DoubleChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 16:
                if (unsigned) {
                    return new CharChunkWriter<>((DoubleChunk<Values> source) -> {
                        final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(source.size());
                        for (int ii = 0; ii < source.size(); ++ii) {
                            chunk.set(ii, QueryLanguageFunctionUtils.charCast(source.get(ii)));
                        }
                        return chunk;
                    }, DoubleChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
                }
                return new ShortChunkWriter<>((DoubleChunk<Values> source) -> {
                    final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.shortCast(source.get(ii)));
                    }
                    return chunk;
                }, DoubleChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 32:
                return new IntChunkWriter<>((DoubleChunk<Values> source) -> {
                    final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.intCast(source.get(ii)));
                    }
                    return chunk;
                }, DoubleChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            case 64:
                return new LongChunkWriter<>((DoubleChunk<Values> source) -> {
                    final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        chunk.set(ii, QueryLanguageFunctionUtils.longCast(source.get(ii)));
                    }
                    return chunk;
                }, DoubleChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unexpected bit width: " + bitWidth);
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
        return new FixedWidthObjectChunkWriter<>(elementWidth, false,
                typeInfo.arrowField().isNullable()) {
            @Override
            protected void writePayload(
                    @NotNull final Context context,
                    @NotNull final DataOutput dos,
                    @NotNull final RowSequence subset) {
                final ObjectChunk<byte[], Values> objectChunk = context.getChunk().asObjectChunk();
                final byte[] nullValue = new byte[elementWidth];
                subset.forAllRowKeys(row -> {
                    byte[] data = objectChunk.get((int) row);
                    if (data == null) {
                        data = nullValue;
                    }
                    if (data.length != elementWidth) {
                        throw new IllegalArgumentException(String.format(
                                "Expected fixed size binary of %d bytes, but got %d bytes when serializing %s",
                                elementWidth, data.length, typeInfo.type().getCanonicalName()));
                    }
                    try {
                        dos.write(data);
                    } catch (final IOException e) {
                        throw new UncheckedDeephavenException(
                                "Unexpected exception while draining data to OutputStream: ", e);
                    }
                });
            }
        };
    }

    private static ChunkWriter<ObjectChunk<ByteVector, Values>> fixedSizeBinaryFromByteVector(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FixedSizeBinary fixedSizeBinary = (ArrowType.FixedSizeBinary) typeInfo.arrowField().getType();
        final int elementWidth = fixedSizeBinary.getByteWidth();
        return new FixedWidthObjectChunkWriter<>(elementWidth, false,
                typeInfo.arrowField().isNullable()) {
            @Override
            protected void writePayload(
                    @NotNull final Context context,
                    @NotNull final DataOutput dos,
                    @NotNull final RowSequence subset) {
                final ObjectChunk<ByteVector, Values> objectChunk = context.getChunk().asObjectChunk();
                final byte[] nullValue = new byte[elementWidth];
                subset.forAllRowKeys(row -> {
                    final ByteVector rowValue = objectChunk.get((int) row);
                    final byte[] data = rowValue == null ? nullValue : rowValue.toArray();
                    if (data.length != elementWidth) {
                        throw new IllegalArgumentException(String.format(
                                "Expected fixed size binary of %d bytes, but got %d bytes when serializing %s",
                                elementWidth, data.length, typeInfo.type().getCanonicalName()));
                    }
                    try {
                        dos.write(data);
                    } catch (final IOException e) {
                        throw new UncheckedDeephavenException(
                                "Unexpected exception while draining data to OutputStream: ", e);
                    }
                });
            }
        };
    }

    private static ChunkWriter<ObjectChunk<ByteBuffer, Values>> fixedSizeBinaryFromByteBuffer(
            final BarrageTypeInfo<Field> typeInfo) {
        final ArrowType.FixedSizeBinary fixedSizeBinary = (ArrowType.FixedSizeBinary) typeInfo.arrowField().getType();
        final int elementWidth = fixedSizeBinary.getByteWidth();
        return new FixedWidthObjectChunkWriter<>(elementWidth, false,
                typeInfo.arrowField().isNullable()) {
            @Override
            protected void writePayload(
                    @NotNull final Context context,
                    @NotNull final DataOutput dos,
                    @NotNull final RowSequence subset) {
                final ObjectChunk<ByteBuffer, Values> objectChunk = context.getChunk().asObjectChunk();
                final byte[] nullValue = new byte[elementWidth];
                subset.forAllRowKeys(row -> {
                    final ByteBuffer rowValue = objectChunk.get((int) row);
                    final byte[] data = rowValue == null ? nullValue : rowValue.array();
                    if (data.length != elementWidth) {
                        throw new IllegalArgumentException(String.format(
                                "Expected fixed size binary of %d bytes, but got %d bytes when serializing %s",
                                elementWidth, data.length, typeInfo.type().getCanonicalName()));
                    }
                    try {
                        dos.write(data);
                    } catch (final IOException e) {
                        throw new UncheckedDeephavenException(
                                "Unexpected exception while draining data to OutputStream: ", e);
                    }
                });
            }
        };
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
                return new IntChunkWriter<>((ObjectChunk<LocalDate, Values> source) -> {
                    final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final LocalDate value = source.get(ii);
                        chunk.set(ii, value == null ? QueryConstants.NULL_INT : (int) value.toEpochDay());
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case MILLISECOND:
                final long factor = Duration.ofDays(1).toMillis();
                return new LongChunkWriter<>((ObjectChunk<LocalDate, Values> source) -> {
                    final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final LocalDate value = source.get(ii);
                        chunk.set(ii, value == null ? QueryConstants.NULL_LONG : value.toEpochDay() * factor);
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected date unit: " + dateType.getUnit());
        }
    }

    private static ChunkWriter<ObjectChunk<Duration, Values>> intervalFromDuration(
            final BarrageTypeInfo<Field> typeInfo) {
        // See intervalFromPeriod's comment for more information on wire format.

        final ArrowType.Interval intervalType = (ArrowType.Interval) typeInfo.arrowField().getType();
        switch (intervalType.getUnit()) {
            case YEAR_MONTH:
            case MONTH_DAY_NANO:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                        "Do not support %s interval from duration as long conversion", intervalType));

            case DAY_TIME:
                final long msPerDay = 24 * 60 * 60 * 1000L;
                return new FixedWidthObjectChunkWriter<>(Integer.BYTES * 2, false, typeInfo.arrowField().isNullable()) {
                    @Override
                    protected void writePayload(
                            @NotNull final Context context,
                            @NotNull final DataOutput dos,
                            @NotNull final RowSequence subset) {
                        final ObjectChunk<Duration, Values> objectChunk = context.getChunk().asObjectChunk();
                        subset.forAllRowKeys(row -> {
                            final Duration value = objectChunk.get((int) row);
                            try {
                                if (value == null) {
                                    dos.writeInt(0);
                                    dos.writeInt(0);
                                } else {
                                    // days then millis
                                    final long millis = value.toMillis();
                                    final long days = millis / msPerDay;
                                    dos.writeInt((int) days);
                                    dos.writeInt((int) (millis % msPerDay));
                                }
                            } catch (final IOException e) {
                                throw new UncheckedDeephavenException(
                                        "Unexpected exception while draining data to OutputStream: ", e);
                            }
                        });
                    }
                };

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected interval unit: " + intervalType.getUnit());
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
                return new IntChunkWriter<>((ObjectChunk<Period, Values> source) -> {
                    final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final Period value = source.get(ii);
                        chunk.set(ii, value == null
                                ? QueryConstants.NULL_INT
                                : value.getMonths() + value.getYears() * 12);
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case DAY_TIME:
                return new FixedWidthObjectChunkWriter<>(Integer.BYTES * 2, false, typeInfo.arrowField().isNullable()) {
                    @Override
                    protected void writePayload(
                            @NotNull final Context context,
                            @NotNull final DataOutput dos,
                            @NotNull final RowSequence subset) {
                        final ObjectChunk<Period, Values> objectChunk = context.getChunk().asObjectChunk();
                        subset.forAllRowKeys(row -> {
                            final Period value = objectChunk.get((int) row);
                            try {
                                if (value == null) {
                                    dos.writeInt(0);
                                    dos.writeInt(0);
                                } else {
                                    // days then millis
                                    dos.writeInt(value.getDays());
                                    dos.writeInt(0);
                                }
                            } catch (final IOException e) {
                                throw new UncheckedDeephavenException(
                                        "Unexpected exception while draining data to OutputStream: ", e);
                            }
                        });
                    }
                };

            case MONTH_DAY_NANO:
                return new FixedWidthObjectChunkWriter<>(Integer.BYTES * 2 + Long.BYTES, false,
                        typeInfo.arrowField().isNullable()) {
                    @Override
                    protected void writePayload(
                            @NotNull final Context context,
                            @NotNull final DataOutput dos,
                            @NotNull final RowSequence subset) {
                        final ObjectChunk<Period, Values> objectChunk = context.getChunk().asObjectChunk();
                        subset.forAllRowKeys(row -> {
                            final Period value = objectChunk.get((int) row);
                            try {
                                if (value == null) {
                                    dos.writeInt(0);
                                    dos.writeInt(0);
                                    dos.writeLong(0);
                                } else {
                                    dos.writeInt(value.getMonths() + value.getYears() * 12);
                                    dos.writeInt(value.getDays());
                                    dos.writeLong(0);
                                }
                            } catch (final IOException e) {
                                throw new UncheckedDeephavenException(
                                        "Unexpected exception while draining data to OutputStream: ", e);
                            }
                        });
                    }
                };

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected interval unit: " + intervalType.getUnit());
        }
    }

    private static ChunkWriter<ObjectChunk<PeriodDuration, Values>> intervalFromPeriodDuration(
            final BarrageTypeInfo<Field> typeInfo) {
        // See intervalToPeriod's comment for more information on wire format.

        final ArrowType.Interval intervalType = (ArrowType.Interval) typeInfo.arrowField().getType();
        switch (intervalType.getUnit()) {
            case YEAR_MONTH:
                return new IntChunkWriter<>((ObjectChunk<PeriodDuration, Values> source) -> {
                    final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(source.size());
                    for (int ii = 0; ii < source.size(); ++ii) {
                        final PeriodDuration value = source.get(ii);
                        chunk.set(ii, value == null ? QueryConstants.NULL_INT
                                : value.getPeriod().getMonths() + value.getPeriod().getYears() * 12);
                    }
                    return chunk;
                }, ObjectChunk::getEmptyChunk, typeInfo.arrowField().isNullable());

            case DAY_TIME:
                final long msPerDay = 24 * 60 * 60 * 1000L;
                return new FixedWidthObjectChunkWriter<>(Integer.BYTES * 2, false,
                        typeInfo.arrowField().isNullable()) {
                    @Override
                    protected void writePayload(
                            @NotNull final Context context,
                            @NotNull final DataOutput dos,
                            @NotNull final RowSequence subset) {
                        final ObjectChunk<PeriodDuration, Values> objectChunk = context.getChunk().asObjectChunk();
                        subset.forAllRowKeys(row -> {
                            final PeriodDuration value =
                                    objectChunk.get((int) row);
                            try {
                                if (value == null) {
                                    dos.writeInt(0);
                                    dos.writeInt(0);
                                } else {
                                    // days then millis
                                    dos.writeInt(value.getPeriod().getDays());
                                    dos.writeInt((int) (value.getDuration().toMillis() % msPerDay));
                                }
                            } catch (final IOException e) {
                                throw new UncheckedDeephavenException(
                                        "Unexpected exception while draining data to OutputStream: ", e);
                            }
                        });
                    }
                };

            case MONTH_DAY_NANO:
                return new FixedWidthObjectChunkWriter<>(Integer.BYTES * 2 + Long.BYTES, false,
                        typeInfo.arrowField().isNullable()) {
                    @Override
                    protected void writePayload(
                            @NotNull final Context context,
                            @NotNull final DataOutput dos,
                            @NotNull final RowSequence subset) {
                        final ObjectChunk<PeriodDuration, Values> objectChunk = context.getChunk().asObjectChunk();
                        subset.forAllRowKeys(row -> {
                            final PeriodDuration value = objectChunk.get((int) row);
                            try {
                                if (value == null) {
                                    dos.writeInt(0);
                                    dos.writeInt(0);
                                    dos.writeLong(0);
                                } else {
                                    final Period period = value.getPeriod();
                                    dos.writeInt(period.getMonths() + period.getYears() * 12);
                                    dos.writeInt(period.getDays());
                                    dos.writeLong(value.getDuration().toNanos());
                                }
                            } catch (final IOException e) {
                                throw new UncheckedDeephavenException(
                                        "Unexpected exception while draining data to OutputStream: ", e);
                            }
                        });
                    }
                };

            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unexpected interval unit: " + intervalType.getUnit());
        }
    }
}
