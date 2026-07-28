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
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.function.Supplier;

public class FloatChunkWriter<SOURCE_CHUNK_TYPE extends Chunk<Values>> extends BaseChunkWriter<SOURCE_CHUNK_TYPE> {
    private static final String DEBUG_NAME = "FloatChunkWriter";

    // Writes a little-endian int into a byte[] at a byte offset in a single (possibly unaligned) store.
    private static final VarHandle LITTLE_ENDIAN_INT =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    // Number of elements encoded per bounded bulk-write window (see BaseChunkWriter#BULK_WRITE_BUFFER_BYTES).
    private static final int BULK_WRITE_ELEMENTS = Math.max(1, BULK_WRITE_BUFFER_BYTES / Float.BYTES);
    private static final FloatChunkWriter<FloatChunk<Values>> NULLABLE_IDENTITY_INSTANCE = new FloatChunkWriter<>(
            null, FloatChunk::getEmptyChunk, true);
    private static final FloatChunkWriter<FloatChunk<Values>> NON_NULLABLE_IDENTITY_INSTANCE = new FloatChunkWriter<>(
            null, FloatChunk::getEmptyChunk, false);

    public static FloatChunkWriter<FloatChunk<Values>> getIdentity(boolean isNullable) {
        return isNullable ? NULLABLE_IDENTITY_INSTANCE : NON_NULLABLE_IDENTITY_INSTANCE;
    }

    public static WritableFloatChunk<Values> chunkUnboxer(
            @NotNull final ObjectChunk<Float, Values> sourceValues) {
        final WritableFloatChunk<Values> output = WritableFloatChunk.makeWritableChunk(sourceValues.size());
        for (int ii = 0; ii < sourceValues.size(); ++ii) {
            output.set(ii, TypeUtils.unbox(sourceValues.get(ii)));
        }
        return output;
    }

    public FloatChunkWriter(
            @Nullable final ChunkTransformer<SOURCE_CHUNK_TYPE> transformer,
            @NotNull final Supplier<SOURCE_CHUNK_TYPE> emptyChunkSupplier,
            final boolean fieldNullable) {
        super(transformer, emptyChunkSupplier, Float.BYTES, true, fieldNullable);
    }

    public static ChunkWriter<ObjectChunk<Float, Values>> makeBoxed(
            @NotNull final ChunkWriter<FloatChunk<Values>> innerWriter) {
        return new ChunkWriter<>() {
            @Override
            public Context makeContext(@NotNull final ObjectChunk<Float, Values> chunk, final long rowOffset) {
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
        return new FloatChunkInputStream(context, subset, options);
    }

    @Override
    protected int computeNullCount(
            @NotNull final Context context,
            @NotNull final RowSequence subset) {
        final MutableInt nullCount = new MutableInt(0);
        final FloatChunk<Values> floatChunk = context.getChunk().asFloatChunk();
        subset.forAllRowKeys(row -> {
            if (floatChunk.isNull((int) row)) {
                nullCount.increment();
            }
        });
        return nullCount.get();
    }

    @Override
    protected void writeValidityBufferInternal(
            @NotNull final Context context,
            @NotNull final RowSequence subset,
            @NotNull final SerContext serContext) {
        final FloatChunk<Values> floatChunk = context.getChunk().asFloatChunk();
        subset.forAllRowKeys(row -> serContext.setNextIsNull(floatChunk.isNull((int) row)));
    }

    private class FloatChunkInputStream extends BaseChunkInputStream<Context> {
        private FloatChunkInputStream(
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

            // write the payload buffer in bounded windows, encoding each value into little-endian bytes and flushing a
            // full window with a single bulk write rather than one DataOutput value (four bytes) at a time
            final FloatChunk<Values> floatChunk = context.getChunk().asFloatChunk();
            final byte[] buffer = new byte[BULK_WRITE_ELEMENTS * Float.BYTES];
            final MutableInt bufferPos = new MutableInt(0);
            subset.forAllRowKeys(row -> {
                LITTLE_ENDIAN_INT.set(buffer, bufferPos.get(), Float.floatToIntBits(floatChunk.get((int) row)));
                bufferPos.add(Float.BYTES);
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
