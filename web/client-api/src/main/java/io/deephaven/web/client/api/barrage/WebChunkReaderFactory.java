//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import elemental2.core.JsDate;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.chunk.BooleanChunkReader;
import io.deephaven.extensions.barrage.chunk.ByteChunkReader;
import io.deephaven.extensions.barrage.chunk.CharChunkReader;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.chunk.ChunkReader;
import io.deephaven.extensions.barrage.chunk.DoubleChunkReader;
import io.deephaven.extensions.barrage.chunk.FloatChunkReader;
import io.deephaven.extensions.barrage.chunk.IntChunkReader;
import io.deephaven.extensions.barrage.chunk.LongChunkReader;
import io.deephaven.extensions.barrage.chunk.ShortChunkReader;
import io.deephaven.extensions.barrage.chunk.VarListChunkReader;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.web.client.api.BigDecimalWrapper;
import io.deephaven.web.client.api.BigIntegerWrapper;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.LocalDateWrapper;
import io.deephaven.web.client.api.LocalTimeWrapper;
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

import java.io.DataInput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PrimitiveIterator;

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
                        if (t.isSigned()) {
                            return new LongChunkReader(options).transform(LongWrapper::of);
                        }
                        throw new IllegalArgumentException("Unsigned 64bit integers not supported");
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
                            totalRows) -> extractChunkFromInputStream(
                                    is,
                                    fieldNodeIter,
                                    bufferInfoIter,
                                    (val, off, len) -> new BigIntegerWrapper(new BigInteger(val, off, len)),
                                    outChunk, outOffset, totalRows);
                }
                if (typeInfo.type() == BigDecimalWrapper.class) {
                    return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                            totalRows) -> extractChunkFromInputStream(
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
                        totalRows) -> extractChunkFromInputStream(is, fieldNodeIter,
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
                        return new LongChunkReader(options).transform(millis -> {
                            if (millis == QueryConstants.NULL_LONG) {
                                return null;
                            }
                            JsDate jsDate = new JsDate((double) (long) millis);
                            return new LocalDateWrapper(jsDate.getUTCFullYear(), 1 + jsDate.getUTCMonth(),
                                    jsDate.getUTCDate());
                        });
                    case DateUnit.DAY:
                        return new IntChunkReader(options).transform(days -> {
                            if (days == QueryConstants.NULL_INT) {
                                return null;
                            }
                            JsDate jsDate = new JsDate(((double) (int) days) * 86400000);
                            return new LocalDateWrapper(jsDate.getUTCFullYear(), 1 + jsDate.getUTCMonth(),
                                    jsDate.getUTCDate());
                        });
                    default:
                        throw new IllegalArgumentException("Unsupported Date unit: " + DateUnit.name(t.unit()));
                }
            }
            case Type.Time: {
                Time t = new Time();
                typeInfo.arrowField().type(t);
                switch (t.bitWidth()) {
                    case 32: {
                        switch (t.unit()) {
                            case TimeUnit.SECOND: {
                                return new IntChunkReader(options)
                                        .transform(LocalTimeWrapper.intCreator(1)::apply);
                            }
                            case TimeUnit.MILLISECOND: {
                                return new IntChunkReader(options)
                                        .transform(LocalTimeWrapper.intCreator(1_000)::apply);
                            }
                            default:
                                throw new IllegalArgumentException("Unsupported Time unit: " + TimeUnit.name(t.unit()));
                        }
                    }
                    case 64: {
                        switch (t.unit()) {
                            case TimeUnit.NANOSECOND: {
                                return new LongChunkReader(options)
                                        .transform(LocalTimeWrapper.longCreator(1_000_000_000)::apply);
                            }
                            case TimeUnit.MICROSECOND: {
                                return new LongChunkReader(options)
                                        .transform(LocalTimeWrapper.longCreator(1_000_000)::apply);
                            }
                            default:
                                throw new IllegalArgumentException("Unsupported Time unit: " + TimeUnit.name(t.unit()));
                        }
                    }
                    default:
                        throw new IllegalArgumentException("Unsupported Time bitWidth: " + t.bitWidth());
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
                            totalRows) -> extractChunkFromInputStream(
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

    public interface Mapper<T> {
        T constructFrom(byte[] buf, int offset, int length) throws IOException;
    }

    public static <T> WritableObjectChunk<T, Values> extractChunkFromInputStream(
            final DataInput is,
            final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final Mapper<T> mapper,
            final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        final ChunkInputStreamGenerator.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.nextLong();
        final long offsetsBuffer = bufferInfoIter.nextLong();
        final long payloadBuffer = bufferInfoIter.nextLong();

        final int numElements = nodeInfo.numElements;
        final WritableObjectChunk<T, Values> chunk;
        if (outChunk != null) {
            chunk = outChunk.asWritableObjectChunk();
        } else {
            final int numRows = Math.max(totalRows, numElements);
            chunk = WritableObjectChunk.makeWritableChunk(numRows);
            chunk.setSize(numRows);
        }

        if (numElements == 0) {
            return chunk;
        }

        final int numValidityWords = (numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityWords);
                final WritableIntChunk<Values> offsets = WritableIntChunk.makeWritableChunk(numElements + 1)) {
            // Read validity buffer:
            int jj = 0;
            for (; jj < Math.min(numValidityWords, validityBuffer / 8); ++jj) {
                isValid.set(jj, is.readLong());
            }
            final long valBufRead = jj * 8L;
            if (valBufRead < validityBuffer) {
                is.skipBytes(LongSizedDataStructure.intSize("VBCISG", validityBuffer - valBufRead));
            }
            // we support short validity buffers
            for (; jj < numValidityWords; ++jj) {
                isValid.set(jj, -1); // -1 is bit-wise representation of all ones
            }

            // Read offsets:
            final long offBufRead = (numElements + 1L) * Integer.BYTES;
            if (offsetsBuffer < offBufRead) {
                throw new IllegalStateException("offset buffer is too short for the expected number of elements");
            }
            for (int i = 0; i < numElements + 1; ++i) {
                offsets.set(i, is.readInt());
            }
            if (offBufRead < offsetsBuffer) {
                is.skipBytes(LongSizedDataStructure.intSize("VBCISG", offsetsBuffer - offBufRead));
            }

            // Read data:
            final int bytesRead = LongSizedDataStructure.intSize("VBCISG", payloadBuffer);
            final byte[] serializedData = new byte[bytesRead];
            is.readFully(serializedData);

            // Deserialize:
            int ei = 0;
            int pendingSkips = 0;

            for (int vi = 0; vi < numValidityWords; ++vi) {
                int bitsLeftInThisWord = Math.min(64, numElements - vi * 64);
                long validityWord = isValid.get(vi);
                do {
                    if ((validityWord & 1) == 1) {
                        if (pendingSkips > 0) {
                            chunk.fillWithNullValue(outOffset + ei, pendingSkips);
                            ei += pendingSkips;
                            pendingSkips = 0;
                        }
                        final int offset = offsets.get(ei);
                        final int length = offsets.get(ei + 1) - offset;
                        Assert.geq(length, "length", 0);
                        if (offset + length > serializedData.length) {
                            throw new IllegalStateException("not enough data was serialized to parse this element: " +
                                    "elementIndex=" + ei + " offset=" + offset + " length=" + length +
                                    " serializedLen=" + serializedData.length);
                        }
                        chunk.set(outOffset + ei++, mapper.constructFrom(serializedData, offset, length));
                        validityWord >>= 1;
                        bitsLeftInThisWord--;
                    } else {
                        final int skips = Math.min(Long.numberOfTrailingZeros(validityWord), bitsLeftInThisWord);
                        pendingSkips += skips;
                        validityWord >>= skips;
                        bitsLeftInThisWord -= skips;
                    }
                } while (bitsLeftInThisWord > 0);
            }

            if (pendingSkips > 0) {
                chunk.fillWithNullValue(outOffset + ei, pendingSkips);
            }
        }

        return chunk;
    }

}
