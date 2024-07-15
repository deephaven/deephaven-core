//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.chunk.BooleanChunkReader;
import io.deephaven.extensions.barrage.chunk.ByteChunkReader;
import io.deephaven.extensions.barrage.chunk.CharChunkReader;
import io.deephaven.extensions.barrage.chunk.ChunkReader;
import io.deephaven.extensions.barrage.chunk.DoubleChunkReader;
import io.deephaven.extensions.barrage.chunk.FloatChunkReader;
import io.deephaven.extensions.barrage.chunk.IntChunkReader;
import io.deephaven.extensions.barrage.chunk.LongChunkReader;
import io.deephaven.extensions.barrage.chunk.ShortChunkReader;
import io.deephaven.extensions.barrage.chunk.VarBinaryChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.chunk.VarListChunkReader;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.BooleanUtils;
import io.deephaven.web.client.api.BigDecimalWrapper;
import io.deephaven.web.client.api.BigIntegerWrapper;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.LongWrapper;
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

/**
 * Browser-compatible implementation of the ChunkReaderFactory, with a focus on reading from arrow types rather than
 * successfully round-tripping to the Java server.
 * <p>
 * Includes some specific workarounds to handle nullability that will make more sense for the browser.
 */
public class WebChunkReaderFactory implements ChunkReader.Factory {
    @Override
    public ChunkReader getReader(StreamReaderOptions options, int factor, ChunkReader.TypeInfo typeInfo) {
        switch (typeInfo.arrowField().typeType()) {
            case Type.Int: {
                Int t = new Int();
                typeInfo.arrowField().type(t);
                switch (t.bitWidth()) {
                    case 8: {
                        return new ByteChunkReader(options);
                    }
                    case 16: {
                        if (t.isSigned()) {
                            return new ShortChunkReader(options);
                        }
                        return new CharChunkReader(options);
                    }
                    case 32: {
                        return new IntChunkReader(options);
                    }
                    case 64: {
                        return new LongChunkReader(options).transform(LongWrapper::of);
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
                        throw new IllegalArgumentException(
                                "Unsupported FloatingPoint precision " + Precision.name(t.precision()));
                }
            }
            case Type.Binary: {
                if (typeInfo.type() == BigIntegerWrapper.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                                    is,
                                    fieldNodeIter,
                                    bufferInfoIter,
                                    (val, off, len) -> new BigIntegerWrapper(new BigInteger(val, off, len)),
                                    outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == BigDecimalWrapper.class) {
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
                                        BigDecimal bigDecimal =
                                                new BigDecimal(new BigInteger(buf, offset + 4, length - 4), scale);
                                        return new BigDecimalWrapper(bigDecimal);
                                    },
                                    outChunk, outOffset, totalRows);
                }
                throw new IllegalArgumentException("Unsupported Binary type " + typeInfo.type());
            }
            case Type.Utf8: {
                return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                        totalRows) -> VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(is, fieldNodeIter,
                                bufferInfoIter, (buf, off, len) -> new String(buf, off, len, StandardCharsets.UTF_8),
                                outChunk, outOffset, totalRows);
            }
            case Type.Bool: {
                BooleanChunkReader subReader = new BooleanChunkReader();
                return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows) -> {
                    try (final WritableByteChunk<Values> inner = (WritableByteChunk<Values>) subReader.readChunk(
                            fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {

                        final WritableObjectChunk<Boolean, Values> chunk;
                        if (outChunk != null) {
                            chunk = outChunk.asWritableObjectChunk();
                        } else {
                            int numRows = Math.max(totalRows, inner.size());
                            chunk = WritableObjectChunk.makeWritableChunk(numRows);
                            chunk.setSize(numRows);
                        }

                        if (outChunk == null) {
                            // if we're not given an output chunk then we better be writing at the front of the new one
                            Assert.eqZero(outOffset, "outOffset");
                        }

                        for (int ii = 0; ii < inner.size(); ++ii) {
                            byte value = inner.get(ii);
                            chunk.set(outOffset + ii, BooleanUtils.byteAsBoolean(value));
                        }

                        return chunk;
                    }

                };
            }
            case Type.Date: {
                Date t = new Date();
                typeInfo.arrowField().type(t);
                switch (t.unit()) {
                    case DateUnit.MILLISECOND:
                        return new LongChunkReader(options).transform(millis -> DateWrapper.of(millis * 1000 * 1000));
                    default:
                        throw new IllegalArgumentException("Unsupported Date unit: " + DateUnit.name(t.unit()));
                }
            }
            case Type.Time: {
                Time t = new Time();
                typeInfo.arrowField().type(t);
                switch (t.bitWidth()) {
                    case TimeUnit.NANOSECOND: {
                        return new LongChunkReader(options).transform(DateWrapper::of);
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
                        return new LongChunkReader(options).transform(DateWrapper::of);
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
