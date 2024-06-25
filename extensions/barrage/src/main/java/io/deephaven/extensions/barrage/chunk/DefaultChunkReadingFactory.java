//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.base.Charsets;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.ColumnConversionMode;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;

import java.io.DataInput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PrimitiveIterator;

import static io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator.MS_PER_DAY;

/**
 * JVM implementation of ChunkReadingFactory, suitable for use in Java clients and servers. This default implementations
 * may not round trip flight types correctly, but will round trip Deephaven table definitions and table data. Neither of
 * these is a required/expected property of being a Flight/Barrage/Deephaven client.
 */
public final class DefaultChunkReadingFactory implements ChunkReadingFactory {
    public static final ChunkReadingFactory INSTANCE = new DefaultChunkReadingFactory();

    @Override
    public WritableChunk<Values> extractChunkFromInputStream(StreamReaderOptions options, int factor,
            ChunkTypeInfo typeInfo, Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
            PrimitiveIterator.OfLong bufferInfoIter, DataInput is, WritableChunk<Values> outChunk, int outOffset,
            int totalRows) throws IOException {
        // TODO (deephaven-core#5453): pass in ArrowType to enable ser/deser of single java class in multiple formats
        switch (typeInfo.chunkType()) {
            case Boolean:
                throw new UnsupportedOperationException("Booleans are reinterpreted as bytes");
            case Char:
                return CharChunkInputStreamGenerator.extractChunkFromInputStream(
                        options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Byte:
                if (typeInfo.type() == Boolean.class || typeInfo.type() == boolean.class) {
                    return BooleanChunkInputStreamGenerator.extractChunkFromInputStream(
                            options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                return ByteChunkInputStreamGenerator.extractChunkFromInputStream(
                        options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Short:
                return ShortChunkInputStreamGenerator.extractChunkFromInputStream(
                        options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Int:
                return IntChunkInputStreamGenerator.extractChunkFromInputStream(
                        options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Long:
                if (factor == 1) {
                    return LongChunkInputStreamGenerator.extractChunkFromInputStream(
                            options,
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                return LongChunkInputStreamGenerator.extractChunkFromInputStreamWithConversion(
                        options,
                        (long v) -> v == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : (v * factor),
                        fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Float:
                return FloatChunkInputStreamGenerator.extractChunkFromInputStream(
                        options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Double:
                return DoubleChunkInputStreamGenerator.extractChunkFromInputStream(
                        options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Object:
                if (typeInfo.type().isArray()) {
                    if (typeInfo.componentType() == byte.class) {
                        return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                                is,
                                fieldNodeIter,
                                bufferInfoIter,
                                (buf, off, len) -> Arrays.copyOfRange(buf, off, off + len),
                                outChunk, outOffset, totalRows);
                    } else {
                        return VarListChunkInputStreamGenerator.extractChunkFromInputStream(options, typeInfo,
                                fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows, this);
                    }
                }
                if (Vector.class.isAssignableFrom(typeInfo.type())) {
                    return VectorChunkInputStreamGenerator.extractChunkFromInputStream(options,
                            typeInfo, fieldNodeIter, bufferInfoIter,
                            is, outChunk, outOffset, totalRows, this);
                }
                if (typeInfo.type() == BigInteger.class) {
                    return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                            is,
                            fieldNodeIter,
                            bufferInfoIter,
                            BigInteger::new,
                            outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == BigDecimal.class) {
                    return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                            is,
                            fieldNodeIter,
                            bufferInfoIter,
                            (final byte[] buf, final int offset, final int length) -> {
                                // read the int scale value as little endian, arrow's endianness.
                                final byte b1 = buf[offset];
                                final byte b2 = buf[offset + 1];
                                final byte b3 = buf[offset + 2];
                                final byte b4 = buf[offset + 3];
                                final int scale = b4 << 24 | (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);
                                return new BigDecimal(new BigInteger(buf, offset + 4, length - 4), scale);
                            },
                            outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Instant.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Long.BYTES, options, io -> {
                                final long value = io.readLong();
                                if (value == QueryConstants.NULL_LONG) {
                                    return null;
                                }
                                return DateTimeUtils.epochNanosToInstant(value * factor);
                            },
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == ZonedDateTime.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Long.BYTES, options, io -> {
                                final long value = io.readLong();
                                if (value == QueryConstants.NULL_LONG) {
                                    return null;
                                }
                                return DateTimeUtils.epochNanosToZonedDateTime(
                                        value * factor, DateTimeUtils.timeZone());
                            },
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Byte.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Byte.BYTES, options, io -> TypeUtils.box(io.readByte()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Character.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Character.BYTES, options, io -> TypeUtils.box(io.readChar()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Double.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Double.BYTES, options, io -> TypeUtils.box(io.readDouble()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Float.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Float.BYTES, options, io -> TypeUtils.box(io.readFloat()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Integer.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Integer.BYTES, options, io -> TypeUtils.box(io.readInt()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Long.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Long.BYTES, options, io -> TypeUtils.box(io.readLong()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Short.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Short.BYTES, options, io -> TypeUtils.box(io.readShort()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == LocalDate.class) {
                    return LongChunkInputStreamGenerator.extractChunkFromInputStreamWithTransform(
                            options,
                            value -> value == QueryConstants.NULL_LONG
                                    ? null
                                    : LocalDate.ofEpochDay(value / MS_PER_DAY),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == LocalTime.class) {
                    return LongChunkInputStreamGenerator.extractChunkFromInputStreamWithTransform(
                            options,
                            value -> value == QueryConstants.NULL_LONG ? null : LocalTime.ofNanoOfDay(value),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == String.class ||
                        options.columnConversionMode().equals(ColumnConversionMode.Stringify)) {
                    return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(is, fieldNodeIter,
                            bufferInfoIter,
                            (buf, off, len) -> new String(buf, off, len, Charsets.UTF_8), outChunk, outOffset,
                            totalRows);
                }
                throw new UnsupportedOperationException(
                        "Do not yet support column conversion mode: " + options.columnConversionMode());
            default:
                throw new UnsupportedOperationException();
        }
    }
}
