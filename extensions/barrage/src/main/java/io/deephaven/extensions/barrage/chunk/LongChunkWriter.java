//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkWriter and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.LongChunk;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

/**
 * Replication source for the other fixed-width primitive writers; see {@code ReplicateBarrageUtils}. Edits here must be
 * followed by {@code ./gradlew replicateBarrageUtils}. Keep this file ASCII-only: the replicator does not round-trip
 * non-ASCII text.
 */
public class LongChunkWriter<SOURCE_CHUNK_TYPE extends Chunk<Values>> extends BaseChunkWriter<SOURCE_CHUNK_TYPE> {
    private static final String DEBUG_NAME = "LongChunkWriter";

    // Number of elements encoded per bounded bulk-write window (see BaseChunkWriter#BULK_WRITE_BUFFER_BYTES).
    private static final int BULK_WRITE_ELEMENTS = Math.max(1, BULK_WRITE_BUFFER_BYTES / Long.BYTES);
    private static final LongChunkWriter<LongChunk<Values>> NULLABLE_IDENTITY_INSTANCE = new LongChunkWriter<>(
            null, LongChunk::getEmptyChunk, true);
    private static final LongChunkWriter<LongChunk<Values>> NON_NULLABLE_IDENTITY_INSTANCE = new LongChunkWriter<>(
            null, LongChunk::getEmptyChunk, false);

    public static LongChunkWriter<LongChunk<Values>> getIdentity(boolean isNullable) {
        return isNullable ? NULLABLE_IDENTITY_INSTANCE : NON_NULLABLE_IDENTITY_INSTANCE;
    }

    public static WritableLongChunk<Values> chunkUnboxer(
            @NotNull final ObjectChunk<Long, Values> sourceValues) {
        final WritableLongChunk<Values> output = WritableLongChunk.makeWritableChunk(sourceValues.size());
        for (int ii = 0; ii < sourceValues.size(); ++ii) {
            output.set(ii, TypeUtils.unbox(sourceValues.get(ii)));
        }
        return output;
    }

    public LongChunkWriter(
            @Nullable final ChunkTransformer<SOURCE_CHUNK_TYPE> transformer,
            @NotNull final Supplier<SOURCE_CHUNK_TYPE> emptyChunkSupplier,
            final boolean fieldNullable) {
        super(transformer, emptyChunkSupplier, Long.BYTES, true, fieldNullable);
    }

    public static ChunkWriter<ObjectChunk<Long, Values>> makeBoxed(
            @NotNull final ChunkWriter<LongChunk<Values>> innerWriter) {
        return new ChunkWriter<>() {
            @Override
            public Context makeContext(@NotNull final ObjectChunk<Long, Values> chunk, final long rowOffset) {
                return innerWriter.makeContext(chunkUnboxer(chunk), rowOffset);
            }

            @Override
            public DrainableColumn getInputStream(@NotNull Context context, @Nullable RowSet subset,
                    @NotNull BarrageOptions options) throws IOException {
                return innerWriter.getInputStream(context, subset, options);
            }

            @Override
            public DrainableColumn getEmptyInputStream(@NotNull BarrageOptions options) throws IOException {
                return innerWriter.getEmptyInputStream(options);
            }

            @Override
            public boolean isFieldNullable() {
                return innerWriter.isFieldNullable();
            }
        };
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final Context context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        return new LongChunkInputStream(context, subset, options);
    }

    @Override
    protected void computeValidity(
            @NotNull final Context context,
            @NotNull final RowSequence subset,
            @NotNull final ValidityBuffer validity) {
        final LongChunk<Values> longChunk = context.getChunk().asLongChunk();
        subset.forAllRowKeys(row -> validity.setNextIsNull(longChunk.isNull((int) row)));
    }

    private class LongChunkInputStream extends BaseChunkInputStream<Context> {
        private LongChunkInputStream(
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
            listener.noteLogicalBuffer(padBufferSize(elementSize * subset.size()));
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (hasBeenRead || subset.isEmpty()) {
                return 0;
            }

            long bytesWritten = 0;
            hasBeenRead = true;
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);

            // write the validity buffer
            bytesWritten += writeValidityBuffer(dos);

            // write the payload buffer in bounded windows, encoding each value into little-endian bytes (via
            // LittleEndianCodec) and flushing a full window with a single bulk write rather than one DataOutput value,
            // i.e. one individual byte write per byte of the value, at a time.
            final LongChunk<Values> longChunk = context.getChunk().asLongChunk();
            final byte[] buffer = new byte[BULK_WRITE_ELEMENTS * Long.BYTES];
            final MutableInt bufferPos = new MutableInt(0);
            subset.forAllRowKeys(row -> {
                LittleEndianCodec.putLong(buffer, bufferPos.get(), longChunk.get((int) row));
                bufferPos.add(Long.BYTES);
                if (bufferPos.get() == buffer.length) {
                    try {
                        outputStream.write(buffer, 0, buffer.length);
                    } catch (final IOException e) {
                        throw new UncheckedDeephavenException(
                                "Unexpected exception while draining data to OutputStream: ", e);
                    }
                    bufferPos.set(0);
                }
            });
            if (bufferPos.get() > 0) {
                outputStream.write(buffer, 0, bufferPos.get());
            }

            bytesWritten += elementSize * subset.size();
            bytesWritten += writePadBuffer(dos, bytesWritten);
            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }
}
