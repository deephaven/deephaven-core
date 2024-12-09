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
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.ByteChunk;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

public class ByteChunkWriter<SOURCE_CHUNK_TYPE extends Chunk<Values>> extends BaseChunkWriter<SOURCE_CHUNK_TYPE> {
    private static final String DEBUG_NAME = "ByteChunkWriter";
    private static final ByteChunkWriter<ByteChunk<Values>> NULLABLE_IDENTITY_INSTANCE = new ByteChunkWriter<>(
            ByteChunk::isNull, ByteChunk::getEmptyChunk, ByteChunk::get, true);
    private static final ByteChunkWriter<ByteChunk<Values>> NON_NULLABLE_IDENTITY_INSTANCE = new ByteChunkWriter<>(
            ByteChunk::isNull, ByteChunk::getEmptyChunk, ByteChunk::get, false);


    public static ByteChunkWriter<ByteChunk<Values>> getIdentity(boolean isNullable) {
        return isNullable ? NULLABLE_IDENTITY_INSTANCE : NON_NULLABLE_IDENTITY_INSTANCE;
    }

    @FunctionalInterface
    public interface ToByteTransformFunction<SourceChunkType extends Chunk<Values>> {
        byte get(SourceChunkType sourceValues, int offset);
    }

    private final ToByteTransformFunction<SOURCE_CHUNK_TYPE> transform;

    public ByteChunkWriter(
            @NotNull final IsRowNullProvider<SOURCE_CHUNK_TYPE> isRowNullProvider,
            @NotNull final Supplier<SOURCE_CHUNK_TYPE> emptyChunkSupplier,
            @Nullable final ToByteTransformFunction<SOURCE_CHUNK_TYPE> transform,
            final boolean fieldNullable) {
        super(isRowNullProvider, emptyChunkSupplier, Byte.BYTES, true, fieldNullable);
        this.transform = transform;
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final Context<SOURCE_CHUNK_TYPE> context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        return new ByteChunkInputStream(context, subset, options);
    }

    private class ByteChunkInputStream extends BaseChunkInputStream<Context<SOURCE_CHUNK_TYPE>> {
        private ByteChunkInputStream(
                @NotNull final Context<SOURCE_CHUNK_TYPE> context,
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
            listener.noteLogicalBuffer(padBufferSize(elementSize * subset.size()));
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
                    dos.writeByte(transform.get(context.getChunk(), (int) row));
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
