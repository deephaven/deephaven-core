//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;

public class ListChunkWriter<LIST_TYPE, COMPONENT_CHUNK_TYPE extends Chunk<Values>>
        extends BaseChunkWriter<ObjectChunk<LIST_TYPE, Values>> {
    private static final String DEBUG_NAME = "ListChunkWriter";

    private final ListChunkReader.Mode mode;
    private final int fixedSizeLength;
    private final ExpansionKernel<LIST_TYPE> kernel;
    private final ChunkWriter<COMPONENT_CHUNK_TYPE> componentWriter;

    public ListChunkWriter(
            final ListChunkReader.Mode mode,
            final int fixedSizeLength,
            final ExpansionKernel<LIST_TYPE> kernel,
            final ChunkWriter<COMPONENT_CHUNK_TYPE> componentWriter,
            final boolean fieldNullable) {
        super(null, ObjectChunk::getEmptyChunk, 0, false, fieldNullable);
        this.mode = mode;
        this.fixedSizeLength = fixedSizeLength;
        this.kernel = kernel;
        this.componentWriter = componentWriter;
    }

    @Override
    protected int computeNullCount(
            @NotNull final ChunkWriter.Context context,
            @NotNull final RowSequence subset) {
        final MutableInt nullCount = new MutableInt(0);
        final ObjectChunk<Object, Values> objectChunk = context.getChunk().asObjectChunk();
        subset.forAllRowKeys(row -> {
            if (objectChunk.isNull((int) row)) {
                nullCount.increment();
            }
        });
        return nullCount.get();
    }

    @Override
    protected void writeValidityBufferInternal(
            @NotNull final ChunkWriter.Context context,
            @NotNull final RowSequence subset,
            @NotNull final SerContext serContext) {
        final ObjectChunk<Object, Values> objectChunk = context.getChunk().asObjectChunk();
        subset.forAllRowKeys(row -> serContext.setNextIsNull(objectChunk.isNull((int) row)));
    }

    @Override
    public Context makeContext(
            @NotNull final ObjectChunk<LIST_TYPE, Values> chunk,
            final long rowOffset) {
        return new Context(chunk, rowOffset);
    }

    public final class Context extends ChunkWriter.Context {
        private final WritableIntChunk<ChunkPositions> offsets;
        private final ChunkWriter.Context innerContext;

        public Context(
                @NotNull final ObjectChunk<LIST_TYPE, Values> chunk,
                final long rowOffset) {
            super(chunk, rowOffset);

            if (mode == ListChunkReader.Mode.FIXED) {
                offsets = null;
            } else {
                int numOffsets = chunk.size() + (mode == ListChunkReader.Mode.VARIABLE ? 1 : 0);
                offsets = WritableIntChunk.makeWritableChunk(numOffsets);
            }

            // noinspection unchecked
            innerContext = componentWriter.makeContext(
                    (COMPONENT_CHUNK_TYPE) kernel.expand(chunk, fixedSizeLength, offsets), 0);
        }

        @Override
        protected void onReferenceCountAtZero() {
            super.onReferenceCountAtZero();
            if (offsets != null) {
                offsets.close();
            }
            innerContext.close();
        }
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final ChunkWriter.Context context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        // noinspection unchecked
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

            final int limit = LongSizedDataStructure.intSize(DEBUG_NAME, options.previewListLengthLimit());
            if ((subset == null || subset.size() == context.size()) && limit == 0) {
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
                final RowSet toUse = mySubset == null ? RowSetFactory.flat(context.getChunk().size()) : mySubset;
                toUse.forAllRowKeys(key -> {

                    int startOffset;
                    int endOffset;
                    if (fixedSizeLength == 0) {
                        startOffset = context.offsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, key));
                        endOffset = context.offsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, key + 1));
                        if (limit < 0) {
                            startOffset = Math.max(startOffset, endOffset + limit);
                        } else if (limit > 0) {
                            endOffset = Math.min(endOffset, startOffset + limit);
                        }
                        myOffsets.add(endOffset - startOffset + myOffsets.get(myOffsets.size() - 1));
                    } else {
                        startOffset = LongSizedDataStructure.intSize(DEBUG_NAME, key * fixedSizeLength);
                        endOffset = LongSizedDataStructure.intSize(DEBUG_NAME, (key + 1) * fixedSizeLength);
                    }
                    if (endOffset > startOffset) {
                        innerSubsetBuilder.appendRange(startOffset, endOffset - 1);
                    }
                });
                if (mySubset == null) {
                    toUse.close();
                }
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
                if (numElements > 0 && mode == ListChunkReader.Mode.VARIABLE) {
                    // we need an extra offset for the end of the last element
                    numOffsetBytes += Integer.BYTES;
                }
                listener.noteLogicalBuffer(padBufferSize(numOffsetBytes));
            }

            // lengths
            if (mode == ListChunkReader.Mode.VIEW) {
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
                    if (numElements > 0 && mode == ListChunkReader.Mode.VARIABLE) {
                        // we need an extra offset for the end of the last element
                        numOffsetBytes += Integer.BYTES;
                    }
                    size += padBufferSize(numOffsetBytes);
                }

                // lengths
                if (mode == ListChunkReader.Mode.VIEW) {
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
            if (hasBeenRead) {
                return 0;
            }

            hasBeenRead = true;
            long bytesWritten = 0;
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);
            // write the validity array with LSB indexing
            bytesWritten += writeValidityBuffer(dos);

            // write offsets array
            if (mode == ListChunkReader.Mode.VARIABLE) {
                // write down only offset (+1) buffer
                final WritableIntChunk<ChunkPositions> offsetsToUse = myOffsets == null ? context.offsets : myOffsets;
                for (int i = 0; i < offsetsToUse.size(); ++i) {
                    dos.writeInt(offsetsToUse.get(i));
                }
                bytesWritten += ((long) offsetsToUse.size()) * Integer.BYTES;
                bytesWritten += writePadBuffer(dos, bytesWritten);
            } else if (mode == ListChunkReader.Mode.VIEW) {
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
