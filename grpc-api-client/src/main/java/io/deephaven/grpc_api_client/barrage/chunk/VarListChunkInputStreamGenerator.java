/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api_client.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.sources.chunk.util.pools.PoolableChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.grpc_api_client.barrage.chunk.array.ArrayExpansionKernel;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

public class VarListChunkInputStreamGenerator<T> extends BaseChunkInputStreamGenerator<ObjectChunk<T, Attributes.Values>> {
    private static final String DEBUG_NAME = "VarListChunkInputStreamGenerator";

    private final Class<T> type;

    private WritableIntChunk<Attributes.ChunkPositions> offsets;
    private ChunkInputStreamGenerator innerGenerator;

    VarListChunkInputStreamGenerator(final Class<T> type, final ObjectChunk<T, Attributes.Values> chunk) {
        super(chunk, 0);
        this.type = type;
    }

    private synchronized void computePayload() {
        if (innerGenerator != null) {
            return;
        }

        final Class<?> componentType = type.getComponentType();
        final ChunkType chunkType = ChunkType.fromElementType(componentType);
        final ArrayExpansionKernel kernel = ArrayExpansionKernel.makeExpansionKernel(chunkType);
        offsets = WritableIntChunk.makeWritableChunk(chunk.size() + 1);

        final WritableChunk<Attributes.Values> innerChunk = kernel.expand(chunk, offsets);
        innerGenerator = ChunkInputStreamGenerator.makeInputStreamGenerator(chunkType, componentType, innerChunk);
    }

    @Override
    public void close() {
        if (REFERENCE_COUNT_UPDATER.decrementAndGet(this) == 0) {
            if (chunk instanceof PoolableChunk) {
                ((PoolableChunk) chunk).close();
            }
            if (offsets != null) {
                offsets.close();
            }
            if (innerGenerator != null) {
                innerGenerator.close();
            }
        }
    }

    @Override
    public DrainableColumn getInputStream(final Options options,
                                          final @Nullable Index subset) throws IOException {
        computePayload();
        return new VarListInputStream(options, subset);
    }

    private class VarListInputStream extends BaseChunkInputStream {
        private int cachedSize = -1;
        private final WritableIntChunk<Attributes.ChunkPositions> myOffsets;
        private final DrainableColumn innerStream;

        private VarListInputStream(
                final Options options, final Index subsetIn) throws IOException {
            super(chunk, options, subsetIn);
            if (subset.size() != offsets.size() - 1) {
                myOffsets = WritableIntChunk.makeWritableChunk(subset.intSize(DEBUG_NAME) + 1);
                myOffsets.set(0, 0);
                final Index.SequentialBuilder myOffsetBuilder = Index.CURRENT_FACTORY.getSequentialBuilder();
                final MutableInt off = new MutableInt();
                subset.forAllLongs(key -> {
                    final int startOffset = offsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, key));
                    final int endOffset = offsets.get(LongSizedDataStructure.intSize(DEBUG_NAME,  key + 1));
                    final int idx = off.incrementAndGet();
                    myOffsets.set(idx, endOffset - startOffset + myOffsets.get(idx - 1));
                    if (endOffset > startOffset) {
                        myOffsetBuilder.appendRange(startOffset, endOffset - 1);
                    }
                });
                try (final Index mySubset = myOffsetBuilder.getIndex()) {
                    innerStream = innerGenerator.getInputStream(options, mySubset);
                }
            } else {
                myOffsets = null;
                innerStream = innerGenerator.getInputStream(options, null);
            }
        }

        private int cachedNullCount = -1;

        @Override
        public int nullCount() {
            if (cachedNullCount == -1) {
                cachedNullCount = 0;
                subset.forAllLongs(i -> {
                    if (chunk.get((int)i) == null) {
                        ++cachedNullCount;
                    }
                });
            }
            return cachedNullCount;
        }

        @Override
        public void visitFieldNodes(final FieldNodeListener listener) {
            listener.noteLogicalFieldNode(subset.intSize(DEBUG_NAME), nullCount());
            innerStream.visitFieldNodes(listener);
        }

        @Override
        public void visitBuffers(final BufferListener listener) {
            // validity
            final int numElements = subset.intSize(DEBUG_NAME);
            listener.noteLogicalBuffer(0, sendValidityBuffer() ? getValidityMapSerializationSizeFor(numElements) : 0);

            // offsets
            long numOffsetBytes = Integer.BYTES * (((long)numElements) + (numElements > 0 ? 1 : 0));
            final long bytesExtended = numOffsetBytes & REMAINDER_MOD_8_MASK;
            if (bytesExtended > 0) {
                numOffsetBytes += 8 - bytesExtended;
            }
            listener.noteLogicalBuffer(0, numOffsetBytes);

            // payload
            innerStream.visitBuffers(listener);
        }

        @Override
        public void close() throws IOException {
            super.close();
            if (myOffsets != null) {
                myOffsets.close();
            }
            innerStream.close();
        }

        @Override
        protected int getRawSize() throws IOException {
            if (cachedSize == -1) {
                // there are n+1 offsets; it is not assumed first offset is zero
                cachedSize = sendValidityBuffer() ? getValidityMapSerializationSizeFor(subset.intSize(DEBUG_NAME)) : 0;
                cachedSize += subset.size() * Integer.BYTES + (subset.isEmpty() ? 0 : Integer.BYTES);

                if (!subset.isEmpty() && (subset.size() & 0x1) == 0) {
                    // then we must also align offset array
                    cachedSize += Integer.BYTES;
                }
                cachedSize += innerStream.available();
            }
            return cachedSize;
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (read || subset.isEmpty()) {
                return 0;
            }

            read = true;
            long bytesWritten = 0;
            try (final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream)) {
                // write the validity array with LSB indexing
                if (sendValidityBuffer()) {
                    final SerContext context = new SerContext();
                    final Runnable flush = () -> {
                        try {
                            dos.writeLong(context.accumulator);
                        } catch (final IOException e) {
                            throw new UncheckedDeephavenException("couldn't drain data to OutputStream", e);
                        }
                        context.accumulator = 0;
                        context.count = 0;
                    };
                    subset.forAllLongs(rawRow -> {
                        final int row = LongSizedDataStructure.intSize(DEBUG_NAME, rawRow);
                        if (chunk.get(row) != null) {
                            context.accumulator |= 1L << context.count;
                        }
                        if (++context.count == 64) {
                            flush.run();
                        }
                    });
                    if (context.count > 0) {
                        flush.run();
                    }
                    bytesWritten += getValidityMapSerializationSizeFor(subset.intSize(DEBUG_NAME));
                }

                // write offsets array
                final WritableIntChunk<Attributes.ChunkPositions> offsetsToUse = myOffsets == null ? offsets : myOffsets;
                for (int i = 0; i < offsetsToUse.size(); ++i) {
                    dos.writeInt(offsetsToUse.get(i));
                }
                bytesWritten += ((long)offsetsToUse.size()) * Integer.BYTES;

                final long bytesExtended = bytesWritten & REMAINDER_MOD_8_MASK;
                if (bytesExtended > 0) {
                    bytesWritten += 8 - bytesExtended;
                    dos.write(PADDING_BUFFER, 0, (int)(8 - bytesExtended));
                }

                bytesWritten += innerStream.drainTo(outputStream);
            }
            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }

    static <T> ObjectChunk<T, Attributes.Values> extractChunkFromInputStream(
            final Options options,
            final Class<T> type,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final Iterator<BufferInfo> bufferInfoIter,
            final DataInput is) throws IOException {

        final FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final BufferInfo validityBuffer = bufferInfoIter.next();
        final BufferInfo offsetsBuffer = bufferInfoIter.next();

        final Class<?> componentType = type.getComponentType();

        if (nodeInfo.numElements == 0) {
            try (final WritableChunk<Attributes.Values> inner = (WritableChunk<Attributes.Values>)ChunkInputStreamGenerator.extractChunkFromInputStream(
                    options, ChunkType.fromElementType(componentType), componentType, fieldNodeIter, bufferInfoIter, is)) {
                return WritableObjectChunk.makeWritableChunk(nodeInfo.numElements);
            }
        }

        final WritableObjectChunk<T, Attributes.Values> chunk;
        final int numValidityLongs = (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Attributes.Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs);
             final WritableIntChunk<Attributes.ChunkPositions> offsets = WritableIntChunk.makeWritableChunk(nodeInfo.numElements + 1)) {
            // Read validity buffer:
            int jj = 0;
            if (validityBuffer.offset > 0) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, validityBuffer.offset));
            }
            for (; jj < Math.min(numValidityLongs, validityBuffer.length / 8); ++jj) {
                isValid.set(jj, is.readLong());
            }
            final long valBufRead = jj * 8L + validityBuffer.offset;
            if (valBufRead < validityBuffer.length) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, validityBuffer.length - valBufRead));
            }
            // we support short validity buffers
            for (; jj < numValidityLongs; ++jj) {
                isValid.set(jj, -1); // -1 is bit-wise representation of all ones
            }
            // consumed entire validity buffer by here

            // Read offsets:
            if (offsetsBuffer.offset > 0) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, offsetsBuffer.offset));
            }
            final long offBufRead = (nodeInfo.numElements + 1L) * Integer.BYTES + offsetsBuffer.offset;
            if (offsetsBuffer.length < offBufRead) {
                throw new IllegalStateException("offset buffer is too short for the expected number of elements");
            }
            for (int i = 0; i < nodeInfo.numElements + 1; ++i) {
                offsets.set(i, is.readInt());
            }
            if (offBufRead < offsetsBuffer.length) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, offsetsBuffer.length - offBufRead));
            }

            final ArrayExpansionKernel kernel = ArrayExpansionKernel.makeExpansionKernel(ChunkType.fromElementType(componentType));
            try (final WritableChunk<Attributes.Values> inner = (WritableChunk<Attributes.Values>)ChunkInputStreamGenerator.extractChunkFromInputStream(
                    options, ChunkType.fromElementType(componentType), componentType, fieldNodeIter, bufferInfoIter, is)) {
                chunk = kernel.contract(inner, offsets);

                long nextValid = 0;
                for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                    if ((ii % 64) == 0) {
                        nextValid = isValid.get(ii / 64);
                    }
                    if ((nextValid & 0x1) == 0x0) {
                        chunk.set(ii, null);
                    }
                    nextValid >>= 1;
                }
            }
        }

        chunk.setSize(nodeInfo.numElements);
        return chunk;
    }
}

