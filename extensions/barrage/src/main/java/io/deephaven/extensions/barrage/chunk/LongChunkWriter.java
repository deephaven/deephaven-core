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
import io.deephaven.chunk.LongChunk;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

public class LongChunkWriter<SOURCE_CHUNK_TYPE extends Chunk<Values>> extends BaseChunkWriter<SOURCE_CHUNK_TYPE> {
    private static final String DEBUG_NAME = "LongChunkWriter";
    private static final LongChunkWriter<LongChunk<Values>> NULLABLE_IDENTITY_INSTANCE = new LongChunkWriter<>(
            LongChunk::isNull, LongChunk::getEmptyChunk, LongChunk::get, true);
    private static final LongChunkWriter<LongChunk<Values>> NON_NULLABLE_IDENTITY_INSTANCE = new LongChunkWriter<>(
            LongChunk::isNull, LongChunk::getEmptyChunk, LongChunk::get, false);


    public static LongChunkWriter<LongChunk<Values>> getIdentity(boolean isNullable) {
        return isNullable ? NULLABLE_IDENTITY_INSTANCE : NON_NULLABLE_IDENTITY_INSTANCE;
    }

    @FunctionalInterface
    public interface ToLongTransformFunction<SourceChunkType extends Chunk<Values>> {
        long get(SourceChunkType sourceValues, int offset);
    }

    private final ToLongTransformFunction<SOURCE_CHUNK_TYPE> transform;

    public LongChunkWriter(
            @NotNull final IsRowNullProvider<SOURCE_CHUNK_TYPE> isRowNullProvider,
            @NotNull final Supplier<SOURCE_CHUNK_TYPE> emptyChunkSupplier,
            @Nullable final ToLongTransformFunction<SOURCE_CHUNK_TYPE> transform,
            final boolean fieldNullable) {
        super(isRowNullProvider, emptyChunkSupplier, Long.BYTES, true, fieldNullable);
        this.transform = transform;
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final Context<SOURCE_CHUNK_TYPE> context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        return new LongChunkInputStream(context, subset, options);
    }

    private class LongChunkInputStream extends BaseChunkInputStream<Context<SOURCE_CHUNK_TYPE>> {
        private LongChunkInputStream(
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
                    dos.writeLong(transform.get(context.getChunk(), (int) row));
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
