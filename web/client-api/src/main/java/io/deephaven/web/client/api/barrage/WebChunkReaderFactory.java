//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import io.deephaven.extensions.barrage.chunk.BooleanChunkReader;
import io.deephaven.extensions.barrage.chunk.ByteChunkReader;
import io.deephaven.extensions.barrage.chunk.CharChunkReader;
import io.deephaven.extensions.barrage.chunk.ChunkReader;
import io.deephaven.extensions.barrage.chunk.ChunkReadingFactory;
import io.deephaven.extensions.barrage.chunk.DoubleChunkReader;
import io.deephaven.extensions.barrage.chunk.FloatChunkReader;
import io.deephaven.extensions.barrage.chunk.IntChunkReader;
import io.deephaven.extensions.barrage.chunk.LongChunkReader;
import io.deephaven.extensions.barrage.chunk.VarBinaryChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.chunk.VarListChunkReader;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import org.apache.arrow.flatbuf.Date;
import org.apache.arrow.flatbuf.DateUnit;
import org.apache.arrow.flatbuf.FloatingPoint;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.flatbuf.Time;
import org.apache.arrow.flatbuf.TimeUnit;
import org.apache.arrow.flatbuf.Timestamp;
import org.apache.arrow.flatbuf.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class WebChunkReaderFactory implements ChunkReadingFactory {
    @Override
    public ChunkReader extractChunkFromInputStream(StreamReaderOptions options, int factor, ChunkTypeInfo typeInfo) {
        switch (typeInfo.arrowField().typeType()) {
            case Type.Int: {
                Int t = new Int();
                typeInfo.arrowField().type(t);
                switch (t.bitWidth()) {
                    case 8: {
                        return new ByteChunkReader(options);
                    }
                    case 16: {
                        return new CharChunkReader(options);
                    }
                    case 32: {
                        return new IntChunkReader(options);
                    }
                    case 64: {
                        return new LongChunkReader(options);
                    }
                    default:
                        throw new IllegalArgumentException("Unsupported Int bitwidth: " + t.bitWidth());
                }
            }
            case Type.FloatingPoint: {
                FloatingPoint t = new FloatingPoint();
                typeInfo.arrowField().type(t);
                switch (t.precision()) {
                    case Precision.SINGLE: {
                        return new FloatChunkReader(options);
                    }
                    case Precision.DOUBLE: {
                        return new DoubleChunkReader(options);
                    }
                    default:
                        throw new IllegalArgumentException("Unsupported FloatingPoint precision " + Precision.name(t.precision()));
                }
            }
            case Type.Binary: {
                if (typeInfo.type() == BigInteger.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                            is,
                            fieldNodeIter,
                            bufferInfoIter,
                            BigInteger::new,
                            outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == BigDecimal.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
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
            }
            case Type.Utf8: {
                return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows) ->
                        VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(is, fieldNodeIter, bufferInfoIter, (buf, off, len) -> new String(buf, off, len, StandardCharsets.UTF_8), outChunk, outOffset, totalRows);
            }
            case Type.Bool: {
                return new BooleanChunkReader();
            }
            case Type.Date: {
                Date t = new Date();
                typeInfo.arrowField().type(t);
                switch (t.unit()) {
                    case DateUnit.MILLISECOND:
                        return new LongChunkReader(options);//TODO transform
                    default:
                        throw new IllegalArgumentException("Unsupported Date unit: " + DateUnit.name(t.unit()));
                }
            }
            case Type.Time: {
                Time t = new Time();
                typeInfo.arrowField().type(t);
                switch (t.bitWidth()) {
                    case TimeUnit.NANOSECOND: {
                        return new LongChunkReader(options);//TODO transform
                    }
                    default:
                        throw new IllegalArgumentException("Unsupported Time unit: " + TimeUnit.name(t.unit()));
                }
            }
            case Type.Timestamp: {
                Timestamp t = new Timestamp();
                typeInfo.arrowField().type(t);
                switch (t.unit()) {
                    case TimeUnit.NANOSECOND: {
                        if (!t.timezone().equals("UTC")) {
                            throw new IllegalArgumentException("Unsupported tz " + t.timezone());
                        }
                        return new LongChunkReader(options);//TODO transform
                    }
                    default:
                        throw new IllegalArgumentException("Unsupported Timestamp unit: " + TimeUnit.name(t.unit()));
                }
            }
            case Type.List: {
                if (typeInfo.componentType() == byte.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                           totalRows) -> VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                            is,
                            fieldNodeIter,
                            bufferInfoIter,
                            (buf, off, len) -> Arrays.copyOfRange(buf, off, off + len),
                            outChunk, outOffset, totalRows);
                }
                return new VarListChunkReader<>(options, typeInfo, this);
            }
            default:
                throw new IllegalArgumentException("Unsupported type: " + Type.name(typeInfo.arrowField().typeType()));
        }
    }
}
