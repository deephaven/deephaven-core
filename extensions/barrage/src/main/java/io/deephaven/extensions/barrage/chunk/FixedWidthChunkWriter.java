//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

public class FixedWidthChunkWriter<SourceChunkType extends Chunk<Values>> extends BaseChunkWriter<SourceChunkType> {
    private static final String DEBUG_NAME = "FixedWidthChunkWriter";

    @FunctionalInterface
    public interface Appender<SourceChunkType extends Chunk<Values>> {
        void append(@NotNull DataOutput os, @NotNull SourceChunkType sourceValues, int offset) throws IOException;
    }

    private final Appender<SourceChunkType> appendItem;

    public FixedWidthChunkWriter(
            @NotNull final Supplier<SourceChunkType> emptyChunkSupplier,
            final int elementSize,
            final boolean dhNullable,
            final Appender<SourceChunkType> appendItem) {
        super(emptyChunkSupplier, elementSize, dhNullable);
        this.appendItem = appendItem;
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final Context<SourceChunkType> context,
            @Nullable final RowSet subset,
            @NotNull final ChunkReader.Options options) throws IOException {
        return new FixedWidthChunkInputStream(context, subset, options);
    }

    private class FixedWidthChunkInputStream extends BaseChunkInputStream<Context<SourceChunkType>> {
        private FixedWidthChunkInputStream(
                @NotNull final Context<SourceChunkType> context,
                @Nullable final RowSet subset,
                @NotNull final ChunkReader.Options options) {
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
            if (read || subset.isEmpty()) {
                return 0;
            }

            long bytesWritten = 0;
            read = true;
            final DataOutput dos = new LittleEndianDataOutputStream(outputStream);

            // write the validity buffer
            bytesWritten += writeValidityBuffer(dos);

            // ensure we can cast all row keys to int
            LongSizedDataStructure.intSize(DEBUG_NAME, subset.lastRowKey());

            // write the payload buffer
            subset.forAllRowKeys(rowKey -> {
                try {
                    appendItem.append(dos, context.getChunk(), (int) rowKey);
                } catch (final IOException e) {
                    throw new UncheckedDeephavenException(
                            "Unexpected exception while draining data to OutputStream: ", e);
                }
            });

            bytesWritten += elementSize * subset.size();
            bytesWritten += writePadBuffer(dos, bytesWritten);
            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }
}
