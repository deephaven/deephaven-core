//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkWriter and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.ShortChunk;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

public class ShortChunkWriter<SourceChunkType extends Chunk<Values>> extends BaseChunkWriter<SourceChunkType> {
    private static final String DEBUG_NAME = "ShortChunkWriter";
    public static final ShortChunkWriter<ShortChunk<Values>> INSTANCE = new ShortChunkWriter<>(
            ShortChunk::getEmptyChunk, ShortChunk::get);

    @FunctionalInterface
    public interface ToShortTransformFunction<SourceChunkType extends Chunk<Values>> {
        short get(SourceChunkType sourceValues, int offset);
    }

    private final ToShortTransformFunction<SourceChunkType> transform;

    public ShortChunkWriter(
            @NotNull final Supplier<SourceChunkType> emptyChunkSupplier,
            @Nullable final ToShortTransformFunction<SourceChunkType> transform) {
        super(emptyChunkSupplier, Short.BYTES, true);
        this.transform = transform;
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final Context<SourceChunkType> context,
            @Nullable final RowSet subset,
            @NotNull final ChunkReader.Options options) throws IOException {
        return new ShortChunkInputStream(context, subset, options);
    }

    private class ShortChunkInputStream extends BaseChunkInputStream<Context<SourceChunkType>> {
        private ShortChunkInputStream(
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
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);

            // write the validity buffer
            bytesWritten += writeValidityBuffer(dos);

            // write the payload buffer
            subset.forAllRowKeys(row -> {
                try {
                    dos.writeShort(transform.get(context.getChunk(), (int) row));
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
