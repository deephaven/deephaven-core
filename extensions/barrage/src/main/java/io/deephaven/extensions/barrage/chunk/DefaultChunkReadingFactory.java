//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.base.Charsets;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.util.ArrowIpcUtil;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;
import org.apache.arrow.vector.types.pojo.Schema;

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
 * JVM implementation of {@link ChunkReader.Factory}, suitable for use in Java clients and servers. This default
 * implementation may not round trip flight types in a stable way, but will round trip Deephaven table definitions and
 * table data. Neither of these is a required/expected property of being a Flight/Barrage/Deephaven client.
 */
public final class DefaultChunkReadingFactory implements ChunkReader.Factory {
    public static final ChunkReader.Factory INSTANCE = new DefaultChunkReadingFactory();

    @Override
    public ChunkReader getReader(StreamReaderOptions options, int factor,
            ChunkReader.TypeInfo typeInfo) {
        // TODO (deephaven-core#5453): pass in ArrowType to enable ser/deser of single java class in multiple formats
        switch (typeInfo.chunkType()) {
            case Boolean:
                throw new UnsupportedOperationException("Booleans are reinterpreted as bytes");
            case Char:
                return new CharChunkReader(options);
            case Byte:
                if (typeInfo.type() == Boolean.class || typeInfo.type() == boolean.class) {
                    return new BooleanChunkReader();
                }
                return new ByteChunkReader(options);
            case Short:
                return new ShortChunkReader(options);
            case Int:
                return new IntChunkReader(options);
            case Long:
                if (factor == 1) {
                    return new LongChunkReader(options);
                }
                return new LongChunkReader(options,
                        (long v) -> v == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : (v * factor));
            case Float:
                return new FloatChunkReader(options);
            case Double:
                return new DoubleChunkReader(options);
            case Object:
                if (typeInfo.type().isArray()) {
                    if (typeInfo.componentType() == byte.class) {
                        return ByteArrayChunkReader.BYTEARRAY_READER;
                    } else {
                        return new VarListChunkReader<>(options, typeInfo, this);
                    }
                }
                if (Vector.class.isAssignableFrom(typeInfo.type())) {
                    return new VectorChunkReader(options, typeInfo, this);
                }
                if (typeInfo.type() == BigInteger.class) {
                    return BigIntegerChunkReader.BIG_INTEGER_CHUNK_READER;
                }
                if (typeInfo.type() == BigDecimal.class) {
                    return BigDecimalChunkReader.BIG_DECIMAL_CHUNK_READER;
                }
                if (typeInfo.type() == Instant.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> FixedWidthChunkInputStreamGenerator
                                    .extractChunkFromInputStreamWithTypeConversion(
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
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> FixedWidthChunkInputStreamGenerator
                                    .extractChunkFromInputStreamWithTypeConversion(
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
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> FixedWidthChunkInputStreamGenerator
                                    .extractChunkFromInputStreamWithTypeConversion(
                                            Byte.BYTES, options, io -> TypeUtils.box(io.readByte()),
                                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Character.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> FixedWidthChunkInputStreamGenerator
                                    .extractChunkFromInputStreamWithTypeConversion(
                                            Character.BYTES, options, io -> TypeUtils.box(io.readChar()),
                                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Double.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> FixedWidthChunkInputStreamGenerator
                                    .extractChunkFromInputStreamWithTypeConversion(
                                            Double.BYTES, options, io -> TypeUtils.box(io.readDouble()),
                                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Float.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> FixedWidthChunkInputStreamGenerator
                                    .extractChunkFromInputStreamWithTypeConversion(
                                            Float.BYTES, options, io -> TypeUtils.box(io.readFloat()),
                                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Integer.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> FixedWidthChunkInputStreamGenerator
                                    .extractChunkFromInputStreamWithTypeConversion(
                                            Integer.BYTES, options, io -> TypeUtils.box(io.readInt()),
                                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Long.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> FixedWidthChunkInputStreamGenerator
                                    .extractChunkFromInputStreamWithTypeConversion(
                                            Long.BYTES, options, io -> TypeUtils.box(io.readLong()),
                                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == Short.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> FixedWidthChunkInputStreamGenerator
                                    .extractChunkFromInputStreamWithTypeConversion(
                                            Short.BYTES, options, io -> TypeUtils.box(io.readShort()),
                                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == LocalDate.class) {
                    return new LongChunkReader(options).transform(value -> value == QueryConstants.NULL_LONG ? null
                            : LocalDate.ofEpochDay(value / MS_PER_DAY));
                }
                if (typeInfo.type() == LocalTime.class) {
                    return new LongChunkReader(options).transform(
                            value -> value == QueryConstants.NULL_LONG ? null : LocalTime.ofNanoOfDay(value));
                }
                if (typeInfo.type() == String.class) {
                    return StringChunkReader.STRING_CHUNK_READER;
                }
                // TODO (core#58): add custom barrage serialization/deserialization support
                if (typeInfo.type() == Schema.class) {
                    // Migrate Schema to custom format when available.
                    return SchemaChunkReader.SCHEMA_CHUNK_READER;
                }
                // All other object types are sent from the server as strings
                return StringChunkReader.STRING_CHUNK_READER;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private enum ByteArrayChunkReader implements ChunkReader {
        BYTEARRAY_READER;

        @Override
        public WritableChunk<Values> readChunk(Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
                PrimitiveIterator.OfLong bufferInfoIter, DataInput is, WritableChunk<Values> outChunk, int outOffset,
                int totalRows) throws IOException {
            return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                    is,
                    fieldNodeIter,
                    bufferInfoIter,
                    ByteArrayChunkReader::readBytes,
                    outChunk,
                    outOffset,
                    totalRows);
        }

        private static byte[] readBytes(byte[] buf, int off, int len) {
            return Arrays.copyOfRange(buf, off, off + len);
        }
    }

    private enum BigIntegerChunkReader implements ChunkReader {
        BIG_INTEGER_CHUNK_READER;

        @Override
        public WritableChunk<Values> readChunk(Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
                PrimitiveIterator.OfLong bufferInfoIter, DataInput is, WritableChunk<Values> outChunk, int outOffset,
                int totalRows) throws IOException {
            return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                    is,
                    fieldNodeIter,
                    bufferInfoIter,
                    BigInteger::new,
                    outChunk,
                    outOffset,
                    totalRows);
        }
    }

    private enum BigDecimalChunkReader implements ChunkReader {
        BIG_DECIMAL_CHUNK_READER;

        @Override
        public WritableChunk<Values> readChunk(Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
                PrimitiveIterator.OfLong bufferInfoIter, DataInput is, WritableChunk<Values> outChunk, int outOffset,
                int totalRows) throws IOException {
            return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                    is,
                    fieldNodeIter,
                    bufferInfoIter,
                    BigDecimalChunkReader::readBigDecimal,
                    outChunk,
                    outOffset,
                    totalRows);
        }

        private static BigDecimal readBigDecimal(byte[] buf, int offset, int length) {
            // read the int scale value as little endian, arrow's endianness.
            final byte b1 = buf[offset];
            final byte b2 = buf[offset + 1];
            final byte b3 = buf[offset + 2];
            final byte b4 = buf[offset + 3];
            final int scale = b4 << 24 | (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);
            return new BigDecimal(new BigInteger(buf, offset + 4, length - 4), scale);
        }
    }

    private enum StringChunkReader implements ChunkReader {
        STRING_CHUNK_READER;

        @Override
        public WritableChunk<Values> readChunk(Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
                PrimitiveIterator.OfLong bufferInfoIter, DataInput is, WritableChunk<Values> outChunk, int outOffset,
                int totalRows) throws IOException {
            return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                    is,
                    fieldNodeIter,
                    bufferInfoIter,
                    StringChunkReader::readString,
                    outChunk,
                    outOffset,
                    totalRows);
        }

        private static String readString(byte[] buf, int off, int len) {
            return new String(buf, off, len, Charsets.UTF_8);
        }
    }

    private enum SchemaChunkReader implements ChunkReader {
        SCHEMA_CHUNK_READER;

        @Override
        public WritableChunk<Values> readChunk(Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
                PrimitiveIterator.OfLong bufferInfoIter, DataInput is, WritableChunk<Values> outChunk, int outOffset,
                int totalRows) throws IOException {
            return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                    is,
                    fieldNodeIter,
                    bufferInfoIter,
                    ArrowIpcUtil::deserialize,
                    outChunk,
                    outOffset,
                    totalRows);
        }
    }
}
