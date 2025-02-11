//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.util.unboxer.ChunkUnboxer;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class MapChunkWriter<T>
        extends BaseChunkWriter<ObjectChunk<T, Values>> {
    private static final String DEBUG_NAME = "MapChunkWriter";

    private final ChunkWriter<Chunk<Values>> keyWriter;
    private final ChunkWriter<Chunk<Values>> valueWriter;
    private final ChunkType keyWriterChunkType;
    private final ChunkType valueWriterChunkType;

    public MapChunkWriter(
            final ChunkWriter<Chunk<Values>> keyWriter,
            final ChunkWriter<Chunk<Values>> valueWriter,
            final ChunkType keyWriterChunkType,
            final ChunkType valueWriterChunkType,
            final boolean fieldNullable) {
        super(null, ObjectChunk::getEmptyChunk, 0, false, fieldNullable);
        this.keyWriter = keyWriter;
        this.valueWriter = valueWriter;
        this.keyWriterChunkType = keyWriterChunkType;
        this.valueWriterChunkType = valueWriterChunkType;
    }

    @Override
    public Context makeContext(
            @NotNull final ObjectChunk<T, Values> chunk,
            final long rowOffset) {
        return new Context(chunk, rowOffset);
    }

    @Override
    protected int computeNullCount(
            @NotNull final BaseChunkWriter.Context context,
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
            @NotNull final BaseChunkWriter.Context context,
            @NotNull final RowSequence subset,
            @NotNull final SerContext serContext) {
        final ObjectChunk<Object, Values> objectChunk = context.getChunk().asObjectChunk();
        subset.forAllRowKeys(row -> serContext.setNextIsNull(objectChunk.isNull((int) row)));
    }

    public final class Context extends ChunkWriter.Context {
        private final WritableIntChunk<ChunkPositions> offsets;
        private final ChunkWriter.Context keyContext;
        private final ChunkWriter.Context valueContext;

        public Context(
                @NotNull final ObjectChunk<T, Values> chunk,
                final long rowOffset) {
            super(chunk, rowOffset);
            // count how big our inner chunks need to be
            int numInnerElements = 0;
            int numOffsets = chunk.size() + 1;
            offsets = WritableIntChunk.makeWritableChunk(numOffsets);
            offsets.setSize(0);
            if (chunk.size() != 0) {
                offsets.add(0);
            }
            for (int ii = 0; ii < chunk.size(); ++ii) {
                final Map<?, ?> row = (Map<?, ?>) chunk.get(ii);
                numInnerElements += row == null ? 0 : row.size();
                offsets.add(numInnerElements);
            }

            final WritableObjectChunk<Object, Values> keyObjChunk =
                    WritableObjectChunk.makeWritableChunk(numInnerElements);
            keyObjChunk.setSize(0);
            final WritableObjectChunk<Object, Values> valueObjChunk =
                    WritableObjectChunk.makeWritableChunk(numInnerElements);
            valueObjChunk.setSize(0);
            for (int ii = 0; ii < chunk.size(); ++ii) {
                final Map<?, ?> row = (Map<?, ?>) chunk.get(ii);
                if (row == null) {
                    continue;
                }
                row.forEach((key, value) -> {
                    keyObjChunk.add(key);
                    valueObjChunk.add(value);
                });
            }

            // unbox keys if necessary
            final Chunk<Values> keyChunk;
            if (keyWriterChunkType == ChunkType.Object) {
                keyChunk = keyObjChunk;
            } else {
                // note that we do not close the unboxer since we steal the inner chunk and pass to key context
                // noinspection unchecked
                keyChunk = (WritableChunk<Values>) ChunkUnboxer.getUnboxer(keyWriterChunkType, keyObjChunk.capacity())
                        .unbox(keyObjChunk);
                keyObjChunk.close();
            }
            keyContext = keyWriter.makeContext(keyChunk, 0);

            // unbox values if necessary
            final Chunk<Values> valueChunk;
            if (valueWriterChunkType == ChunkType.Object) {
                valueChunk = valueObjChunk;
            } else {
                // note that we do not close the unboxer since we steal the inner chunk and pass to value context
                // noinspection unchecked
                valueChunk = (WritableChunk<Values>) ChunkUnboxer
                        .getUnboxer(valueWriterChunkType, valueObjChunk.capacity()).unbox(valueObjChunk);
                valueObjChunk.close();
            }
            valueContext = valueWriter.makeContext(valueChunk, 0);
        }

        @Override
        protected void onReferenceCountAtZero() {
            super.onReferenceCountAtZero();
            offsets.close();
            keyContext.close();
            valueContext.close();
        }
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final ChunkWriter.Context context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        // noinspection unchecked
        return new MapChunkInputStream((Context) context, subset, options);
    }

    private class MapChunkInputStream extends BaseChunkInputStream<Context> {

        private int cachedSize = -1;
        private final WritableIntChunk<ChunkPositions> myOffsets;
        private final DrainableColumn keyColumn;
        private final DrainableColumn valueColumn;

        private MapChunkInputStream(
                @NotNull final Context context,
                @Nullable final RowSet mySubset,
                @NotNull final BarrageOptions options) throws IOException {
            super(context, mySubset, options);

            if (subset == null || subset.size() == context.size()) {
                // we are writing everything
                myOffsets = null;
                keyColumn = keyWriter.getInputStream(context.keyContext, null, options);
                valueColumn = valueWriter.getInputStream(context.valueContext, null, options);
            } else {
                // note that we maintain dense offsets within the writer, but write per the wire format
                myOffsets = WritableIntChunk.makeWritableChunk(context.size() + 1);
                myOffsets.setSize(0);
                myOffsets.add(0);

                final RowSetBuilderSequential innerSubsetBuilder = RowSetFactory.builderSequential();
                subset.forAllRowKeys(key -> {
                    final int startOffset = context.offsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, key));
                    final int endOffset = context.offsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, key + 1));
                    myOffsets.add(endOffset - startOffset + myOffsets.get(myOffsets.size() - 1));
                    if (endOffset > startOffset) {
                        innerSubsetBuilder.appendRange(startOffset, endOffset - 1);
                    }
                });
                try (final RowSet innerSubset = innerSubsetBuilder.build()) {
                    keyColumn = keyWriter.getInputStream(context.keyContext, innerSubset, options);
                    valueColumn = valueWriter.getInputStream(context.valueContext, innerSubset, options);
                }
            }
        }

        @Override
        public void visitFieldNodes(final FieldNodeListener listener) {
            // map type has a logical node
            listener.noteLogicalFieldNode(subset.intSize(DEBUG_NAME), nullCount());
            // inner type also has a logical node
            if (myOffsets == null) {
                listener.noteLogicalFieldNode(context.offsets.size(), nullCount());
            } else {
                listener.noteLogicalFieldNode(myOffsets.size(), nullCount());
            }
            keyColumn.visitFieldNodes(listener);
            valueColumn.visitFieldNodes(listener);
        }

        @Override
        public void visitBuffers(final BufferListener listener) {
            // validity
            final int numElements = subset.intSize(DEBUG_NAME);
            listener.noteLogicalBuffer(sendValidityBuffer() ? getValidityMapSerializationSizeFor(numElements) : 0);

            // offsets
            long numOffsetBytes = Integer.BYTES * ((long) numElements);
            if (numElements > 0) {
                // we need an extra offset for the end of the last element
                numOffsetBytes += Integer.BYTES;
            }
            listener.noteLogicalBuffer(padBufferSize(numOffsetBytes));

            // a validity buffer for the inner struct; (the buffer is odd since each entry needs to be valid)
            listener.noteLogicalBuffer(0);

            // payload
            keyColumn.visitBuffers(listener);
            valueColumn.visitBuffers(listener);
        }

        @Override
        public void close() throws IOException {
            super.close();
            if (myOffsets != null) {
                myOffsets.close();
            }
            keyColumn.close();
            valueColumn.close();
        }

        @Override
        protected int getRawSize() throws IOException {
            if (cachedSize == -1) {
                long size;

                // validity
                final int numElements = subset.intSize(DEBUG_NAME);
                size = sendValidityBuffer() ? getValidityMapSerializationSizeFor(numElements) : 0;

                // offsets
                long numOffsetBytes = Integer.BYTES * ((long) numElements);
                if (numElements > 0) {
                    // we need an extra offset for the end of the last element
                    numOffsetBytes += Integer.BYTES;
                }
                size += padBufferSize(numOffsetBytes);

                size += keyColumn.available();
                size += valueColumn.available();
                cachedSize = LongSizedDataStructure.intSize(DEBUG_NAME, size);
            }

            return cachedSize;
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (hasBeenRead || subset.isEmpty()) {
                return 0;
            }

            hasBeenRead = true;
            long bytesWritten = 0;
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);
            // write the validity array with LSB indexing
            bytesWritten += writeValidityBuffer(dos);

            // write offsets array
            final WritableIntChunk<ChunkPositions> offsetsToUse = myOffsets == null ? context.offsets : myOffsets;
            for (int i = 0; i < offsetsToUse.size(); ++i) {
                dos.writeInt(offsetsToUse.get(i));
            }
            bytesWritten += ((long) offsetsToUse.size()) * Integer.BYTES;
            bytesWritten += writePadBuffer(dos, bytesWritten);

            bytesWritten += keyColumn.drainTo(outputStream);
            bytesWritten += valueColumn.drainTo(outputStream);
            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }
}
