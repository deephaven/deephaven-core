//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.extensions.barrage.chunk.array.ArrayExpansionKernel;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;

public class VarListChunkInputStreamGenerator<T> extends BaseChunkInputStreamGenerator<ObjectChunk<T, Values>> {
    private static final String DEBUG_NAME = "VarListChunkInputStreamGenerator";

    private final Factory factory;
    private final Class<T> type;

    private WritableIntChunk<ChunkPositions> offsets;
    private ChunkInputStreamGenerator innerGenerator;

    VarListChunkInputStreamGenerator(ChunkInputStreamGenerator.Factory factory, final Class<T> type,
            final ObjectChunk<T, Values> chunk, final long rowOffset) {
        super(chunk, 0, rowOffset);
        this.factory = factory;
        this.type = type;
    }

    private synchronized void computePayload() {
        if (innerGenerator != null) {
            return;
        }

        final Class<?> myType = type.getComponentType();
        final Class<?> myComponentType = myType != null ? myType.getComponentType() : null;

        final ChunkType chunkType;
        if (myType == boolean.class || myType == Boolean.class) {
            // Note: Internally booleans are passed around as bytes, but the wire format is packed bits.
            chunkType = ChunkType.Byte;
        } else if (myType != null && !myType.isPrimitive()) {
            chunkType = ChunkType.Object;
        } else {
            chunkType = ChunkType.fromElementType(myType);
        }

        final ArrayExpansionKernel kernel = ArrayExpansionKernel.makeExpansionKernel(chunkType, myType);
        offsets = WritableIntChunk.makeWritableChunk(chunk.size() + 1);

        final WritableChunk<Values> innerChunk = kernel.expand(chunk, offsets);
        innerGenerator = factory.makeInputStreamGenerator(chunkType, myType, myComponentType, innerChunk, 0);
    }

    @Override
    protected void onReferenceCountAtZero() {
        super.onReferenceCountAtZero();
        if (offsets != null) {
            offsets.close();
        }
        if (innerGenerator != null) {
            innerGenerator.close();
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

}

