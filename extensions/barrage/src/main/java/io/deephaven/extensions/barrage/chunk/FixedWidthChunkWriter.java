//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

public abstract class FixedWidthChunkWriter<SOURCE_CHUNK_TYPE extends Chunk<Values>>
        extends BaseChunkWriter<SOURCE_CHUNK_TYPE> {
    private static final String DEBUG_NAME = "FixedWidthChunkWriter";

    public FixedWidthChunkWriter(
            @Nullable final ChunkTransformer<SOURCE_CHUNK_TYPE> transformer,
            @NotNull final Supplier<SOURCE_CHUNK_TYPE> emptyChunkSupplier,
            final int elementSize,
            final boolean dhNullable,
            final boolean fieldNullable) {
        super(transformer, emptyChunkSupplier, elementSize, dhNullable, fieldNullable);
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final Context context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        return new FixedWidthChunkInputStream(context, subset, options);
    }

    protected abstract void writePayload(
            @NotNull final Context context,
            @NotNull final DataOutput dos,
            @NotNull final RowSequence subset);

    private class FixedWidthChunkInputStream extends BaseChunkInputStream<Context> {
        private FixedWidthChunkInputStream(
                @NotNull final Context context,
                @Nullable final RowSet subset,
                @NotNull final BarrageOptions options) {
            super(context, subset, options);
        }

        @Override
        public void visitFieldNodes(final FieldNodeListener listener) {
            listener.noteLogicalFieldNode(subset.intSize(DEBUG_NAME), nullCount());
        }

        @Override
        public void visitBuffers(final BufferListener listener) {
            // validity
            listener.noteLogicalBuffer(sendValidityBuffer() ? getValidityMapSerializationSizeFor(subset.intSize()) : 0);
            // payload
            long length = elementSize * subset.size();
            listener.noteLogicalBuffer(padBufferSize(length));
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (hasBeenRead || subset.isEmpty()) {
                return 0;
            }

            long bytesWritten = 0;
            hasBeenRead = true;
            final DataOutput dos = new LittleEndianDataOutputStream(outputStream);

            // write the validity buffer
            bytesWritten += writeValidityBuffer(dos);

            // ensure we can cast all row keys to int
            LongSizedDataStructure.intSize(DEBUG_NAME, subset.lastRowKey());

            // write the payload buffer
            writePayload(context, dos, subset);

            bytesWritten += elementSize * subset.size();
            bytesWritten += writePadBuffer(dos, bytesWritten);
            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }
}
