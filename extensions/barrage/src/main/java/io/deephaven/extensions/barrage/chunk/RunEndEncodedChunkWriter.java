//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.rpc.Code;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Serializes a flat column chunk as Arrow Run-End Encoded (REE) on the wire.
 *
 * <p>
 * The REE parent array has logical length {@code numRows}, null_count 0, and <em>zero</em> buffers. It has two
 * children:
 * <ol>
 * <li>{@code run_ends} — a non-nullable Int16/Int32/Int64 array of {@code numRuns} cumulative 1-based end indices (last
 * == numRows)</li>
 * <li>{@code values} — the encoded value type of length {@code numRuns}, one representative value per run</li>
 * </ol>
 *
 * <p>
 * Runs are computed over the selected subset. Consecutive equal values (including null == null) collapse into a single
 * run.
 */
public class RunEndEncodedChunkWriter extends BaseChunkWriter<Chunk<Values>> {
    private static final String DEBUG_NAME = "RunEndEncodedChunkWriter";

    private final ChunkWriter<IntChunk<Values>> runEndsWriter;
    private final ChunkWriter<Chunk<Values>> valuesWriter;
    private final ChunkType runEndsChunkType;
    private final ChunkType valuesChunkType;
    private final BarrageRunKernel runKernel;

    public RunEndEncodedChunkWriter(
            @NotNull final ChunkWriter<IntChunk<Values>> runEndsWriter,
            @NotNull final ChunkWriter<Chunk<Values>> valuesWriter,
            @NotNull final ChunkType runEndsChunkType,
            @NotNull final ChunkType valuesChunkType,
            final boolean fieldNullable) {
        super(null, ObjectChunk::getEmptyChunk, 0, false, fieldNullable);
        this.runEndsWriter = runEndsWriter;
        this.valuesWriter = valuesWriter;
        this.runEndsChunkType = runEndsChunkType;
        this.valuesChunkType = valuesChunkType;
        this.runKernel = BarrageRunKernel.makeBarrageRunKernel(valuesChunkType);
    }

    @Override
    protected int computeNullCount(
            @NotNull final ChunkWriter.Context context,
            @NotNull final RowSequence subset) {
        // The REE parent array never contains nulls; nullability lives in the values child.
        return 0;
    }

    @Override
    protected void writeValidityBufferInternal(
            @NotNull final ChunkWriter.Context context,
            @NotNull final RowSequence subset,
            @NotNull final SerContext serContext) {
        // REE parent has no validity buffer.
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final ChunkWriter.Context context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        return new RunEndEncodedChunkInputStream(context, subset, options, null);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Forwards {@code dictionaryRegistry} to the {@code values} child writer so that a dictionary-encoded values child
     * (yielding a {@code RunEndEncoded<Dictionary<...>>} column) can resolve and register its dictionary state. The
     * {@code run_ends} child is never dictionary-encoded, so it ignores the registry.
     */
    @Override
    public DrainableColumn getInputStream(
            @NotNull final ChunkWriter.Context context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options,
            @Nullable final DictionaryWriterRegistry dictionaryRegistry) throws IOException {
        return new RunEndEncodedChunkInputStream(context, subset, options, dictionaryRegistry);
    }

    private class RunEndEncodedChunkInputStream extends BaseChunkInputStream<ChunkWriter.Context> {
        private int cachedSize = -1;

        // child contexts and drainable columns — owned and closed by this stream
        private final ChunkWriter.Context runEndsCtx;
        private final ChunkWriter.Context valuesCtx;
        private final DrainableColumn runEndsColumn;
        private final DrainableColumn valuesColumn;

        RunEndEncodedChunkInputStream(
                @NotNull final ChunkWriter.Context context,
                @Nullable final RowSet mySubset,
                @NotNull final BarrageOptions options,
                @Nullable final DictionaryWriterRegistry dictionaryRegistry) throws IOException {
            super(context, mySubset, options);

            final int logicalSize = subset.intSize(DEBUG_NAME);

            // Fast path for empty subset.
            if (logicalSize == 0) {
                final WritableIntChunk<Values> emptyRunEnds = WritableIntChunk.makeWritableChunk(0);
                final WritableChunk<Values> emptyValues = valuesChunkType.makeWritableChunk(0);
                this.runEndsCtx = runEndsWriter.makeContext(emptyRunEnds, 0);
                this.valuesCtx = valuesWriter.makeContext(emptyValues, 0);
                this.runEndsColumn = runEndsWriter.getInputStream(runEndsCtx, null, options);
                this.valuesColumn = valuesWriter.getInputStream(valuesCtx, null, options, dictionaryRegistry);
                return;
            }

            // Validate: the last run_end value equals logicalSize, so logicalSize must fit in the index type.
            checkRunEndsOverflow(logicalSize, runEndsChunkType);

            // Single-pass run detection directly from the source chunk over the subset.
            // Pre-allocate worst-case capacity; the kernel resets sizes to zero and uses add().
            final WritableIntChunk<Values> tempRunEnds = WritableIntChunk.makeWritableChunk(logicalSize);
            final WritableChunk<Values> tempRunValues = valuesChunkType.makeWritableChunk(logicalSize);
            runKernel.encodeRunEnds(context.getChunk(), subset, tempRunEnds, tempRunValues);

            // Create child contexts (contexts own the chunks and close them on ref-count zero).
            this.runEndsCtx = runEndsWriter.makeContext(tempRunEnds, 0);
            this.valuesCtx = valuesWriter.makeContext(tempRunValues, 0);
            this.runEndsColumn = runEndsWriter.getInputStream(runEndsCtx, null, options);
            // Forward the registry so a dictionary-encoded values child (REE<Dictionary<...>>) registers its state.
            this.valuesColumn = valuesWriter.getInputStream(valuesCtx, null, options, dictionaryRegistry);
        }

        @Override
        public void visitFieldNodes(final FieldNodeListener listener) {
            // Parent REE field node: logical length = subset size, null_count = 0 (REE spec).
            listener.noteLogicalFieldNode(subset.intSize(DEBUG_NAME), nullCount());
            // Child field nodes follow.
            runEndsColumn.visitFieldNodes(listener);
            valuesColumn.visitFieldNodes(listener);
        }

        @Override
        public void visitBuffers(final BufferListener listener) {
            // REE parent has ZERO buffers (no validity, no offsets).
            // Children contribute their own buffers.
            runEndsColumn.visitBuffers(listener);
            valuesColumn.visitBuffers(listener);
        }

        @Override
        protected int getRawSize() throws IOException {
            if (cachedSize == -1) {
                cachedSize = LongSizedDataStructure.intSize(DEBUG_NAME,
                        (long) runEndsColumn.available() + valuesColumn.available());
            }
            return cachedSize;
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (hasBeenRead || subset.isEmpty()) {
                return 0;
            }
            hasBeenRead = true;
            // REE parent contributes no bytes — drain children only.
            long bytesWritten = 0;
            bytesWritten += runEndsColumn.drainTo(outputStream);
            bytesWritten += valuesColumn.drainTo(outputStream);
            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }

        @Override
        public void close() throws IOException {
            super.close();
            runEndsColumn.close();
            valuesColumn.close();
            runEndsCtx.close();
            valuesCtx.close();
        }
    }

    /**
     * Throws INVALID_ARGUMENT if the logical length {@code logicalSize} cannot be represented as a run_end value in the
     * given index type. Because run_ends stores cumulative end indices and the last value always equals logicalSize,
     * the index type must be wide enough to hold logicalSize itself.
     */
    static void checkRunEndsOverflow(final int logicalSize, final ChunkType runEndsChunkType) {
        if (runEndsChunkType == ChunkType.Short && logicalSize > Short.MAX_VALUE) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Run-end encoded batch has " + logicalSize
                            + " logical rows, which exceeds the Int16 run_ends maximum of " + Short.MAX_VALUE
                            + ". Use Int32 or Int64 run_ends instead.");
        }
        // Note: Int32 and Int64 cannot overflow for any Deephaven batch (which is int-bounded).
    }
}
