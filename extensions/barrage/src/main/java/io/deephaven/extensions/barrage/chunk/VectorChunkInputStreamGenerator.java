//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.extensions.barrage.chunk.vector.VectorExpansionKernel;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.vector.Vector;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class VectorChunkInputStreamGenerator extends BaseChunkInputStreamGenerator<ObjectChunk<Vector<?>, Values>> {
    private static final String DEBUG_NAME = "VarListChunkInputStreamGenerator";

    private final Class<?> componentType;

    private WritableIntChunk<ChunkPositions> offsets;
    private ChunkInputStreamGenerator innerGenerator;

    VectorChunkInputStreamGenerator(
            final Class<Vector<?>> type,
            final Class<?> componentType,
            final ObjectChunk<Vector<?>, Values> chunk,
            final long rowOffset) {
        super(chunk, 0, rowOffset);
        this.componentType = VectorExpansionKernel.getComponentType(type, componentType);
    }

    private synchronized void computePayload() {
        if (innerGenerator != null) {
            return;
        }

        final Class<?> innerComponentType = componentType != null ? componentType.getComponentType() : null;
        final ChunkType chunkType = ChunkType.fromElementType(componentType);
        final VectorExpansionKernel kernel = VectorExpansionKernel.makeExpansionKernel(chunkType, componentType);
        offsets = WritableIntChunk.makeWritableChunk(chunk.size() + 1);

        final WritableChunk<Values> innerChunk = kernel.expand(chunk, offsets);
        innerGenerator = ChunkInputStreamGenerator.makeInputStreamGenerator(
                chunkType, componentType, innerComponentType, innerChunk, 0);
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
    public DrainableColumn getInputStream(final StreamReaderOptions options,
            @Nullable final RowSet subset) throws IOException {
        computePayload();
        return new VarListInputStream(options, subset);
    }

    private class VarListInputStream extends BaseChunkInputStream {
        private int cachedSize = -1;
        private final WritableIntChunk<ChunkPositions> myOffsets;
        private final DrainableColumn innerStream;

        private VarListInputStream(
                final StreamReaderOptions options, final RowSet subsetIn) throws IOException {
            super(chunk, options, subsetIn);
            if (subset.size() != offsets.size() - 1) {
                myOffsets = WritableIntChunk.makeWritableChunk(subset.intSize(DEBUG_NAME) + 1);
                myOffsets.set(0, 0);
                final RowSetBuilderSequential myOffsetBuilder = RowSetFactory.builderSequential();
                final MutableInt off = new MutableInt();
                subset.forAllRowKeys(key -> {
                    final int startOffset = offsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, key));
                    final int endOffset = offsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, key + 1));
                    final int idx = off.incrementAndGet();
                    myOffsets.set(idx, endOffset - startOffset + myOffsets.get(idx - 1));
                    if (endOffset > startOffset) {
                        myOffsetBuilder.appendRange(startOffset, endOffset - 1);
                    }
                });
                try (final RowSet mySubset = myOffsetBuilder.build()) {
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
                subset.forAllRowKeys(i -> {
                    if (chunk.get((int) i) == null) {
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
            listener.noteLogicalBuffer(sendValidityBuffer() ? getValidityMapSerializationSizeFor(numElements) : 0);

            // offsets
            long numOffsetBytes = Integer.BYTES * (((long) numElements) + (numElements > 0 ? 1 : 0));
            final long bytesExtended = numOffsetBytes & REMAINDER_MOD_8_MASK;
            if (bytesExtended > 0) {
                numOffsetBytes += 8 - bytesExtended;
            }
            listener.noteLogicalBuffer(numOffsetBytes);

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
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);
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
                subset.forAllRowKeys(rawRow -> {
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
            final WritableIntChunk<ChunkPositions> offsetsToUse = myOffsets == null ? offsets : myOffsets;
            for (int i = 0; i < offsetsToUse.size(); ++i) {
                dos.writeInt(offsetsToUse.get(i));
            }
            bytesWritten += ((long) offsetsToUse.size()) * Integer.BYTES;

            final long bytesExtended = bytesWritten & REMAINDER_MOD_8_MASK;
            if (bytesExtended > 0) {
                bytesWritten += 8 - bytesExtended;
                dos.write(PADDING_BUFFER, 0, (int) (8 - bytesExtended));
            }

            bytesWritten += innerStream.drainTo(outputStream);
            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }

    static WritableObjectChunk<Vector<?>, Values> extractChunkFromInputStream(
            final StreamReaderOptions options,
            final Class<Vector<?>> type,
            final Class<?> inComponentType,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {

        final FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.nextLong();
        final long offsetsBuffer = bufferInfoIter.nextLong();

        final Class<?> componentType = VectorExpansionKernel.getComponentType(type, inComponentType);
        final ChunkType chunkType = ChunkType.fromElementType(componentType);

        if (nodeInfo.numElements == 0) {
            try (final WritableChunk<Values> ignored = ChunkInputStreamGenerator.extractChunkFromInputStream(
                    options, chunkType, componentType, componentType.getComponentType(), fieldNodeIter, bufferInfoIter,
                    is,
                    null, 0, 0)) {
                if (outChunk != null) {
                    return outChunk.asWritableObjectChunk();
                }
                return WritableObjectChunk.makeWritableChunk(totalRows);
            }
        }

        final WritableObjectChunk<Vector<?>, Values> chunk;
        final int numValidityLongs = (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs);
                final WritableIntChunk<ChunkPositions> offsets =
                        WritableIntChunk.makeWritableChunk(nodeInfo.numElements + 1)) {
            // Read validity buffer:
            int jj = 0;
            for (; jj < Math.min(numValidityLongs, validityBuffer / 8); ++jj) {
                isValid.set(jj, is.readLong());
            }
            final long valBufRead = jj * 8L;
            if (valBufRead < validityBuffer) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, validityBuffer - valBufRead));
            }
            // we support short validity buffers
            for (; jj < numValidityLongs; ++jj) {
                isValid.set(jj, -1); // -1 is bit-wise representation of all ones
            }
            // consumed entire validity buffer by here

            // Read offsets:
            final long offBufRead = (nodeInfo.numElements + 1L) * Integer.BYTES;
            if (offsetsBuffer < offBufRead) {
                throw new IllegalStateException("offset buffer is too short for the expected number of elements");
            }
            for (int i = 0; i < nodeInfo.numElements + 1; ++i) {
                offsets.set(i, is.readInt());
            }
            if (offBufRead < offsetsBuffer) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, offsetsBuffer - offBufRead));
            }

            final VectorExpansionKernel kernel = VectorExpansionKernel.makeExpansionKernel(chunkType, componentType);
            try (final WritableChunk<Values> inner = ChunkInputStreamGenerator.extractChunkFromInputStream(
                    options, chunkType, componentType, componentType.getComponentType(), fieldNodeIter, bufferInfoIter,
                    is,
                    null, 0, 0)) {
                chunk = kernel.contract(inner, offsets, outChunk, outOffset, totalRows);

                long nextValid = 0;
                for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                    if ((ii % 64) == 0) {
                        nextValid = isValid.get(ii / 64);
                    }
                    if ((nextValid & 0x1) == 0x0) {
                        chunk.set(outOffset + ii, null);
                    }
                    nextValid >>= 1;
                }
            }
        }

        return chunk;
    }
}
