//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;

public class ListChunkWriter<ListType, ComponentChunkType extends Chunk<Values>>
        extends BaseChunkWriter<ObjectChunk<ListType, Values>> {
    private static final String DEBUG_NAME = "ListChunkWriter";

    private final ListChunkReader.Mode mode;
    private final int fixedSizeLength;
    private final ExpansionKernel<ListType> kernel;
    private final ChunkWriter<ComponentChunkType> componentWriter;

    public ListChunkWriter(
            final ListChunkReader.Mode mode,
            final int fixedSizeLength,
            final ExpansionKernel<ListType> kernel,
            final ChunkWriter<ComponentChunkType> componentWriter) {
        super(ObjectChunk::isNull, ObjectChunk::getEmptyChunk, 0, false);
        this.mode = mode;
        this.fixedSizeLength = fixedSizeLength;
        this.kernel = kernel;
        this.componentWriter = componentWriter;
    }

    @Override
    public Context makeContext(
            @NotNull final ObjectChunk<ListType, Values> chunk,
            final long rowOffset) {
        return new Context(chunk, rowOffset);
    }

    public final class Context extends ChunkWriter.Context<ObjectChunk<ListType, Values>> {
        private final WritableIntChunk<ChunkPositions> offsets;
        private final ChunkWriter.Context<ComponentChunkType> innerContext;

        public Context(
                @NotNull final ObjectChunk<ListType, Values> chunk,
                final long rowOffset) {
            super(chunk, rowOffset);

            if (mode == ListChunkReader.Mode.FIXED) {
                offsets = null;
            } else {
                int numOffsets = chunk.size() + (mode == ListChunkReader.Mode.DENSE ? 1 : 0);
                offsets = WritableIntChunk.makeWritableChunk(numOffsets);
            }

            // noinspection unchecked
            innerContext = componentWriter.makeContext(
                    (ComponentChunkType) kernel.expand(chunk, fixedSizeLength, offsets), 0);
        }

        @Override
        protected void onReferenceCountAtZero() {
            super.onReferenceCountAtZero();
            offsets.close();
            innerContext.close();
        }
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final ChunkWriter.Context<ObjectChunk<ListType, Values>> context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        return new ListChunkInputStream((Context) context, subset, options);
    }

    private class ListChunkInputStream extends BaseChunkInputStream<Context> {

        private int cachedSize = -1;
        private final WritableIntChunk<ChunkPositions> myOffsets;
        private final DrainableColumn innerColumn;

        private ListChunkInputStream(
                @NotNull final Context context,
                @Nullable final RowSet mySubset,
                @NotNull final BarrageOptions options) throws IOException {
            super(context, mySubset, options);

            if (subset == null || subset.size() == context.size()) {
                // we are writing everything
                myOffsets = null;
                innerColumn = componentWriter.getInputStream(context.innerContext, null, options);
            } else {
                if (fixedSizeLength != 0) {
                    myOffsets = null;
                } else {
                    // note that we maintain dense offsets within the writer, but write per the wire format
                    myOffsets = WritableIntChunk.makeWritableChunk(context.size() + 1);
                    myOffsets.setSize(0);
                    myOffsets.add(0);
                }

                final RowSetBuilderSequential innerSubsetBuilder = RowSetFactory.builderSequential();
                subset.forAllRowKeys(key -> {
                    final int startOffset = context.offsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, key));
                    final int endOffset = context.offsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, key + 1));
                    if (fixedSizeLength == 0) {
                        myOffsets.add(endOffset - startOffset + myOffsets.get(myOffsets.size() - 1));
                    }
                    if (endOffset > startOffset) {
                        innerSubsetBuilder.appendRange(startOffset, endOffset - 1);
                    }
                });
                try (final RowSet innerSubset = innerSubsetBuilder.build()) {
                    innerColumn = componentWriter.getInputStream(context.innerContext, innerSubset, options);
                }
            }
        }

        @Override
        public void visitFieldNodes(final FieldNodeListener listener) {
            listener.noteLogicalFieldNode(subset.intSize(DEBUG_NAME), nullCount());
            innerColumn.visitFieldNodes(listener);
        }

        @Override
        public void visitBuffers(final BufferListener listener) {
            // validity
            final int numElements = subset.intSize(DEBUG_NAME);
            listener.noteLogicalBuffer(sendValidityBuffer() ? getValidityMapSerializationSizeFor(numElements) : 0);

            // offsets
            if (mode != ListChunkReader.Mode.FIXED) {
                long numOffsetBytes = Integer.BYTES * ((long) numElements);
                if (numElements > 0 && mode == ListChunkReader.Mode.DENSE) {
                    // we need an extra offset for the end of the last element
                    numOffsetBytes += Integer.BYTES;
                }
                listener.noteLogicalBuffer(padBufferSize(numOffsetBytes));
            }

            // lengths
            if (mode == ListChunkReader.Mode.SPARSE) {
                long numLengthsBytes = Integer.BYTES * ((long) numElements);
                listener.noteLogicalBuffer(padBufferSize(numLengthsBytes));
            }

            // payload
            innerColumn.visitBuffers(listener);
        }

        @Override
        public void close() throws IOException {
            super.close();
            if (myOffsets != null) {
                myOffsets.close();
            }
            innerColumn.close();
        }

        @Override
        protected int getRawSize() throws IOException {
            if (cachedSize == -1) {
                long size;

                // validity
                final int numElements = subset.intSize(DEBUG_NAME);
                size = sendValidityBuffer() ? getValidityMapSerializationSizeFor(numElements) : 0;

                // offsets
                if (mode != ListChunkReader.Mode.FIXED) {
                    long numOffsetBytes = Integer.BYTES * ((long) numElements);
                    if (numElements > 0 && mode == ListChunkReader.Mode.DENSE) {
                        // we need an extra offset for the end of the last element
                        numOffsetBytes += Integer.BYTES;
                    }
                    size += padBufferSize(numOffsetBytes);
                }

                // lengths
                if (mode == ListChunkReader.Mode.SPARSE) {
                    long numLengthsBytes = Integer.BYTES * ((long) numElements);
                    size += padBufferSize(numLengthsBytes);
                }

                size += innerColumn.available();
                cachedSize = LongSizedDataStructure.intSize(DEBUG_NAME, size);
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
            bytesWritten += writeValidityBuffer(dos);

            // write offsets array
            if (mode == ListChunkReader.Mode.DENSE) {
                // write down only offset (+1) buffer
                final WritableIntChunk<ChunkPositions> offsetsToUse = myOffsets == null ? context.offsets : myOffsets;
                for (int i = 0; i < offsetsToUse.size(); ++i) {
                    dos.writeInt(offsetsToUse.get(i));
                }
                bytesWritten += ((long) offsetsToUse.size()) * Integer.BYTES;
                bytesWritten += writePadBuffer(dos, bytesWritten);
            } else if (mode == ListChunkReader.Mode.SPARSE) {
                // write down offset buffer
                final WritableIntChunk<ChunkPositions> offsetsToUse = myOffsets == null ? context.offsets : myOffsets;

                // note that we have one extra offset because we keep dense offsets internally
                for (int i = 0; i < offsetsToUse.size() - 1; ++i) {
                    dos.writeInt(offsetsToUse.get(i));
                }
                bytesWritten += ((long) offsetsToUse.size() - 1) * Integer.BYTES;
                bytesWritten += writePadBuffer(dos, bytesWritten);

                // write down length buffer
                for (int i = 0; i < offsetsToUse.size() - 1; ++i) {
                    dos.writeInt(offsetsToUse.get(i + 1) - offsetsToUse.get(i));
                }
                bytesWritten += ((long) offsetsToUse.size() - 1) * Integer.BYTES;
                bytesWritten += writePadBuffer(dos, bytesWritten);
            } // the other mode is fixed, which doesn't have an offset or length buffer

            bytesWritten += innerColumn.drainTo(outputStream);
            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }
}
