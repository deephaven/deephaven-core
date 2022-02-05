/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage.chunk;

import com.google.common.base.Charsets;
import gnu.trove.iterator.TLongIterator;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.ColumnConversionMode;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;

public interface ChunkInputStreamGenerator extends SafeCloseable {

    static <T> ChunkInputStreamGenerator makeInputStreamGenerator(
            final ChunkType chunkType, final Class<T> type, final Chunk<Values> chunk) {
        switch (chunkType) {
            case Boolean:
                throw new UnsupportedOperationException("Booleans are reinterpreted as bytes");
            case Char:
                return new CharChunkInputStreamGenerator(chunk.asCharChunk(), Character.BYTES);
            case Byte:
                return new ByteChunkInputStreamGenerator(chunk.asByteChunk(), Byte.BYTES);
            case Short:
                return new ShortChunkInputStreamGenerator(chunk.asShortChunk(), Short.BYTES);
            case Int:
                return new IntChunkInputStreamGenerator(chunk.asIntChunk(), Integer.BYTES);
            case Long:
                return new LongChunkInputStreamGenerator(chunk.asLongChunk(), Long.BYTES);
            case Float:
                return new FloatChunkInputStreamGenerator(chunk.asFloatChunk(), Float.BYTES);
            case Double:
                return new DoubleChunkInputStreamGenerator(chunk.asDoubleChunk(), Double.BYTES);
            case Object:
                if (type.isArray()) {
                    return new VarListChunkInputStreamGenerator<>(type, chunk.asObjectChunk());
                }
                if (type == String.class) {
                    return new VarBinaryChunkInputStreamGenerator<>(String.class, chunk.asObjectChunk(), (out, str) -> {
                        out.write(str.getBytes(Charsets.UTF_8));
                    });
                }
                if (type == BigInteger.class) {
                    return new VarBinaryChunkInputStreamGenerator<>(BigInteger.class, chunk.asObjectChunk(), (out, item) -> {
                        out.write(item.toByteArray());
                    });
                }
                if (type == BigDecimal.class) {
                    return new VarBinaryChunkInputStreamGenerator<>(BigDecimal.class, chunk.asObjectChunk(), (out, item) -> {
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
                // TODO (core#936): support column conversion modes

                return new VarBinaryChunkInputStreamGenerator<>(type, chunk.asObjectChunk(), (out, item) -> {
                    out.write(item.toString().getBytes(Charsets.UTF_8));
                });
            default:
                throw new UnsupportedOperationException();
        }
    }

    static <T> Chunk<Values> extractChunkFromInputStream(
            final StreamReaderOptions options,
            final ChunkType chunkType, final Class<T> type,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final TLongIterator bufferInfoIter,
            final DataInput is) throws IOException {
        return extractChunkFromInputStream(options, 1, chunkType, type, fieldNodeIter, bufferInfoIter, is);
    }

    static <T> Chunk<Values> extractChunkFromInputStream(
            final StreamReaderOptions options,
            final int factor,
            final ChunkType chunkType, final Class<T> type,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final TLongIterator bufferInfoIter,
            final DataInput is) throws IOException {
        switch (chunkType) {
            case Boolean:
                throw new UnsupportedOperationException("Booleans are reinterpreted as bytes");
            case Char:
                return CharChunkInputStreamGenerator.extractChunkFromInputStream(
                        Character.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Byte:
                return ByteChunkInputStreamGenerator.extractChunkFromInputStream(
                        Byte.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Short:
                return ShortChunkInputStreamGenerator.extractChunkFromInputStream(
                        Short.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Int:
                return IntChunkInputStreamGenerator.extractChunkFromInputStream(
                        Integer.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Long:
                if (factor == 1) {
                    return LongChunkInputStreamGenerator.extractChunkFromInputStream(
                            Long.BYTES, options,
                            fieldNodeIter, bufferInfoIter, is);
                }
                return LongChunkInputStreamGenerator.extractChunkFromInputStreamWithConversion(
                        Long.BYTES, options,
                        (long v) -> (v*factor),
                        fieldNodeIter, bufferInfoIter, is);
            case Float:
                return FloatChunkInputStreamGenerator.extractChunkFromInputStream(
                        Float.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Double:
                return DoubleChunkInputStreamGenerator.extractChunkFromInputStream(
                        Double.BYTES, options,fieldNodeIter, bufferInfoIter, is);
            case Object:
                if (type.isArray()) {
                   return VarListChunkInputStreamGenerator.extractChunkFromInputStream(
                           options, type, fieldNodeIter, bufferInfoIter, is);
                }
                if (type == BigInteger.class) {
                    return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                            is,
                            fieldNodeIter,
                            bufferInfoIter,
                            BigInteger::new
                    );
                }
                if (type == BigDecimal.class) {
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
                            }
                    );
                }
                if (type == String.class ||
                        options.columnConversionMode().equals(ColumnConversionMode.Stringify)) {
                    return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(is, fieldNodeIter, bufferInfoIter,
                            (buf, off, len) -> new String(buf, off, len, Charsets.UTF_8));
                }
                throw new UnsupportedOperationException("Do not yet support column conversion mode: " + options.columnConversionMode());
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Get an input stream optionally position-space filtered using the provided RowSet.
     *
     * @param options the serializable options for this subscription
     * @param subset if provided, is a position-space filter of source data
     * @return a single-use DrainableColumn ready to be drained via grpc
     */
    DrainableColumn getInputStream(final StreamReaderOptions options, @Nullable final RowSet subset) throws IOException;

    final class FieldNodeInfo {
        public final int numElements;
        public final int nullCount;

        public FieldNodeInfo(final int numElements, final int nullCount) {
            this.numElements = numElements;
            this.nullCount = nullCount;
        }

        public FieldNodeInfo(final org.apache.arrow.flatbuf.FieldNode node) {
            this(LongSizedDataStructure.intSize("FieldNodeInfo", node.length()),
                    LongSizedDataStructure.intSize("FieldNodeInfo", node.nullCount()));
        }
    }

    final class BufferInfo {
        public final long length;

        public BufferInfo(final long length) {
            this.length = length;
        }
    }

    @FunctionalInterface
    interface FieldNodeListener {
        void noteLogicalFieldNode(final int numElements, final int nullCount);
    }

    @FunctionalInterface
    interface BufferListener {
        void noteLogicalBuffer(final long length);
    }

    abstract class DrainableColumn extends DefensiveDrainable {
        /**
         * Append the field nde to the flatbuffer payload via the supplied listener.
         * @param listener the listener to notify for each logical field node in this payload
         */
        public abstract void visitFieldNodes(final FieldNodeListener listener);

        /**
         * Append the buffer boundaries to the flatbuffer payload via the supplied listener.
         * @param listener the listener to notify for each sub-buffer in this payload
         */
        public abstract void visitBuffers(final BufferListener listener);

        /**
         * Count the number of null elements in the outer-most layer of this column (i.e. does not count nested nulls inside of arrays)
         * @return the number of null elements -- 'useDeephavenNulls' counts are always 0 so that we may omit the validity buffer
         */
        public abstract int nullCount();
    }
}
