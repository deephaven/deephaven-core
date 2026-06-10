//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.rpc.Code;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

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

    private final ChunkWriter<Chunk<Values>> runEndsWriter;
    private final ChunkWriter<Chunk<Values>> valuesWriter;
    private final ChunkType runEndsChunkType;
    private final ChunkType valuesChunkType;

    public RunEndEncodedChunkWriter(
            @NotNull final ChunkWriter<Chunk<Values>> runEndsWriter,
            @NotNull final ChunkWriter<Chunk<Values>> valuesWriter,
            @NotNull final ChunkType runEndsChunkType,
            @NotNull final ChunkType valuesChunkType,
            final boolean fieldNullable) {
        super(null, ObjectChunk::getEmptyChunk, 0, false, fieldNullable);
        this.runEndsWriter = runEndsWriter;
        this.valuesWriter = valuesWriter;
        this.runEndsChunkType = runEndsChunkType;
        this.valuesChunkType = valuesChunkType;
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
        return new RunEndEncodedChunkInputStream(context, subset, options);
    }

    private class RunEndEncodedChunkInputStream extends BaseChunkInputStream<ChunkWriter.Context> {
        private int cachedSize = -1;

        // temp dense projection — owned and closed by this stream (null when no projection needed)
        @Nullable
        private final WritableChunk<Values> denseValues;
        // child contexts and drainable columns — owned and closed by this stream
        private final ChunkWriter.Context runEndsCtx;
        private final ChunkWriter.Context valuesCtx;
        private final DrainableColumn runEndsColumn;
        private final DrainableColumn valuesColumn;

        RunEndEncodedChunkInputStream(
                @NotNull final ChunkWriter.Context context,
                @Nullable final RowSet mySubset,
                @NotNull final BarrageOptions options) throws IOException {
            super(context, mySubset, options);

            final int logicalSize = subset.intSize(DEBUG_NAME);

            // Fast path for empty subset: no projection or run detection needed.
            if (logicalSize == 0) {
                this.denseValues = null;
                // noinspection unchecked
                final WritableChunk<Values> emptyRunEnds = (WritableChunk<Values>) makeRunEndsChunk(0);
                final WritableChunk<Values> emptyValues = valuesChunkType.makeWritableChunk(0);
                this.runEndsCtx = runEndsWriter.makeContext(emptyRunEnds, 0);
                this.valuesCtx = valuesWriter.makeContext(emptyValues, 0);
                this.runEndsColumn = runEndsWriter.getInputStream(runEndsCtx, null, options);
                this.valuesColumn = valuesWriter.getInputStream(valuesCtx, null, options);
                return;
            }

            // Validate: the last run_end value equals logicalSize, so logicalSize must fit in the index type.
            checkRunEndsOverflow(logicalSize, runEndsChunkType);

            // Determine the dense source chunk for run detection.
            final WritableChunk<Values> tempDense;
            final Chunk<Values> srcForRuns;
            if (subset.size() == context.size()) {
                // Use the full source chunk directly — no projection needed.
                tempDense = null;
                srcForRuns = context.getChunk();
            } else {
                // Project the subset into a contiguous dense temp chunk.
                tempDense = valuesChunkType.makeWritableChunk(logicalSize);
                tempDense.setSize(logicalSize);
                // Array over MutableInt for efficiency.
                final int[] destPos = {0};
                subset.forAllRowKeyRanges((start, end) -> {
                    final int rangeLength = (int) (end - start + 1);
                    tempDense.copyFromChunk(context.getChunk(), (int) start, destPos[0], rangeLength);
                    destPos[0] += rangeLength;
                });
                srcForRuns = tempDense;
            }
            this.denseValues = tempDense;

            // Detect run boundaries using typed element-wise equality (no boxing).
            final WritableChunk<Values> tempRunEnds;
            final WritableChunk<Values> tempRunValues;
            try (final WritableBooleanChunk<Values> isEqualNext =
                    WritableBooleanChunk.makeWritableChunk(logicalSize - 1)) {
                // NB: ChunkEquals delegates the Comparisons class, so will handle off-nominal
                // values properly.
                ChunkEquals.makeEqual(valuesChunkType).equalNext(srcForRuns, isEqualNext);

                // Count runs: starts at 1, increments at each inequality boundary.
                int numRuns = 1;
                for (int i = 0; i < isEqualNext.size(); ++i) {
                    if (!isEqualNext.get(i)) {
                        ++numRuns;
                    }
                }

                // noinspection unchecked
                tempRunEnds = (WritableChunk<Values>) makeRunEndsChunk(numRuns);
                tempRunValues = valuesChunkType.makeWritableChunk(numRuns);

                // Fill run_ends (cumulative 1-based end indices) and per-run value representatives.
                int runIndex = 0;
                int runStart = 0;
                for (int i = 0; i < isEqualNext.size(); ++i) {
                    if (!isEqualNext.get(i)) {
                        setRunEnd(tempRunEnds, runIndex, i + 1);
                        tempRunValues.copyFromChunk(srcForRuns, runStart, runIndex, 1);
                        ++runIndex;
                        runStart = i + 1;
                    }
                }
                // Last run: ends at logicalSize.
                setRunEnd(tempRunEnds, runIndex, logicalSize);
                tempRunValues.copyFromChunk(srcForRuns, runStart, runIndex, 1);
                tempRunEnds.setSize(numRuns);
                tempRunValues.setSize(numRuns);
            }

            // Create child contexts (contexts own the chunks and close them on ref-count zero).
            this.runEndsCtx = runEndsWriter.makeContext(tempRunEnds, 0);
            this.valuesCtx = valuesWriter.makeContext(tempRunValues, 0);
            this.runEndsColumn = runEndsWriter.getInputStream(runEndsCtx, null, options);
            this.valuesColumn = valuesWriter.getInputStream(valuesCtx, null, options);
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
            if (denseValues != null) {
                denseValues.close();
            }
            runEndsColumn.close();
            valuesColumn.close();
            runEndsCtx.close();
            valuesCtx.close();
        }
    }

    // ---- static helpers ----

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

    /**
     * Allocate a run_ends chunk whose element type matches {@link #runEndsChunkType} with capacity {@code numRuns}.
     */
    @VisibleForTesting
    WritableChunk<?> makeRunEndsChunk(final int numRuns) {
        switch (runEndsChunkType) {
            case Short:
                return WritableShortChunk.makeWritableChunk(numRuns);
            case Int:
                return WritableIntChunk.makeWritableChunk(numRuns);
            case Long:
                return WritableLongChunk.makeWritableChunk(numRuns);
            default:
                throw new IllegalArgumentException("run_ends ChunkType must be Short, Int, or Long; got: "
                        + runEndsChunkType);
        }
    }

    /**
     * Set run_ends[r] = value, dispatching on the underlying chunk type.
     */
    @VisibleForTesting
    static void setRunEnd(
            final WritableChunk<Values> runEnds, final int r, final int value) {
        switch (runEnds.getChunkType()) {
            case Short:
                runEnds.asWritableShortChunk().set(r, (short) value);
                break;
            case Int:
                runEnds.asWritableIntChunk().set(r, value);
                break;
            case Long:
                runEnds.asWritableLongChunk().set(r, value);
                break;
            default:
                throw new IllegalStateException("Unexpected run_ends ChunkType: " + runEnds.getChunkType());
        }
    }
}
