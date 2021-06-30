/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api_client.barrage.chunk;

import com.google.common.base.Charsets;
import io.deephaven.barrage.flatbuf.BarrageFieldNode;
import io.deephaven.barrage.flatbuf.Buffer;
import io.deephaven.barrage.flatbuf.FieldNode;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.util.SafeCloseable;
import io.grpc.Drainable;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public interface ChunkInputStreamGenerator extends SafeCloseable {
    class Options {
        public final boolean isViewport;
        public final boolean useDeephavenNulls;

        private Options(final boolean isViewport, final boolean useDeephavenNulls) {
            this.isViewport = isViewport;
            this.useDeephavenNulls = useDeephavenNulls;
        }

        public static class Builder {
            private boolean isViewport;
            private boolean useDeephavenNulls;

            public Builder setIsViewport(final boolean isViewport) {
                this.isViewport = isViewport;
                return this;
            }

            public Builder setUseDeephavenNulls(final boolean useDeephavenNulls) {
                this.useDeephavenNulls = useDeephavenNulls;
                return this;
            }

            public Options build() {
                return new Options(isViewport, useDeephavenNulls);
            }
        }
    }

    static <T> ChunkInputStreamGenerator makeInputStreamGenerator(final ChunkType chunkType, final Class<T> type, final Chunk<Attributes.Values> chunk) {
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
                } else if (type == String.class) {
                    return new VarBinaryChunkInputStreamGenerator<>(String.class, chunk.asObjectChunk(), (out, str) -> {
                        out.write(str.getBytes(Charsets.UTF_8));
                    });
                }
                // TODO (core#513): BigDecimal, BigInteger

                return new VarBinaryChunkInputStreamGenerator<>(type, chunk.asObjectChunk(), (out, item) -> {
                    out.write(item.toString().getBytes(Charsets.UTF_8));
                });
            default:
                throw new UnsupportedOperationException();
        }
    }

    static <T> Chunk<Attributes.Values> extractChunkFromInputStream(
            final Options options,
            final ChunkType chunkType, final Class<T> type,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final Iterator<BufferInfo> bufferInfoIter,
            final DataInput is) throws IOException {
        switch (chunkType) {
            case Boolean:
                throw new UnsupportedOperationException("Booleans are reinterpreted as bytes");
            case Char:
                return CharChunkInputStreamGenerator.extractChunkFromInputStream(Character.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Byte:
                return ByteChunkInputStreamGenerator.extractChunkFromInputStream(Byte.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Short:
                return ShortChunkInputStreamGenerator.extractChunkFromInputStream(Short.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Int:
                return IntChunkInputStreamGenerator.extractChunkFromInputStream(Integer.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Long:
                return LongChunkInputStreamGenerator.extractChunkFromInputStream(Long.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Float:
                return FloatChunkInputStreamGenerator.extractChunkFromInputStream(Float.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Double:
                return DoubleChunkInputStreamGenerator.extractChunkFromInputStream(Double.BYTES, options, fieldNodeIter, bufferInfoIter, is);
            case Object:
                if (type.isArray()) {
                   return VarListChunkInputStreamGenerator.extractChunkFromInputStream(options, type, fieldNodeIter, bufferInfoIter, is) ;
                }

                return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(is, fieldNodeIter, bufferInfoIter,
                        (buf, off, len) -> new String(buf, off, len, Charsets.UTF_8));
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Get an input stream optionally position-space filtered using the provided index.
     *
     * @param options the serializable options for this subscription
     * @param subset if provided, is a position-space filter of source data
     * @return a single-use DrainableColumn ready to be drained via grpc
     */
    DrainableColumn getInputStream(final Options options, @Nullable final Index subset) throws IOException;

    final class FieldNodeInfo {
        public final int numElements;
        public final int nullCount;

        public FieldNodeInfo(final int numElements, final int nullCount) {
            this.numElements = numElements;
            this.nullCount = nullCount;
        }

        public FieldNodeInfo(final FieldNode node) {
            this(LongSizedDataStructure.intSize("FieldNodeInfo", node.length()),
                    LongSizedDataStructure.intSize("FieldNodeInfo", node.nullCount()));
        }

        public FieldNodeInfo(final BarrageFieldNode node) {
            this(LongSizedDataStructure.intSize("FieldNodeInfo", node.length()),
                    LongSizedDataStructure.intSize("FieldNodeInfo", node.nullCount()));
        }
    }

    final class BufferInfo {
        public final long offset;
        public final long length;

        public BufferInfo(final long offset, final long length) {
            this.offset = offset;
            this.length = length;
        }

        public BufferInfo(final Buffer node) {
            this(LongSizedDataStructure.intSize("BufferInfo", node.offset()),
                    LongSizedDataStructure.intSize("BufferInfo", node.length()));
        }
    }

    @FunctionalInterface
    interface FieldNodeListener {
        void noteLogicalFieldNode(final int numElements, final int nullCount);
    }

    @FunctionalInterface
    interface BufferListener {
        void noteLogicalBuffer(final long offset, final long length);
    }

    abstract class DrainableColumn extends InputStream implements Drainable {
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
