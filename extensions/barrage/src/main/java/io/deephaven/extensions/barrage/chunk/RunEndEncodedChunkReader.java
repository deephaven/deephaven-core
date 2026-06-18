//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

import java.util.function.IntUnaryOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 * Reads an Arrow Run-End Encoded (REE) column from the wire into a flat Deephaven chunk.
 *
 * <p>
 * The REE parent array has logical length {@code numRows}, null_count 0, and <em>zero</em> buffers. The reader consumes
 * one parent {@link ChunkWriter.FieldNodeInfo} (no parent buffers), then reads the two children:
 * <ol>
 * <li>{@code run_ends} — non-nullable Int16/Int32/Int64, {@code numRuns} cumulative 1-based end indices (last ==
 * numRows)</li>
 * <li>{@code values} — the encoded value type, one value per run</li>
 * </ol>
 * The expanded flat output chunk has length {@code numRows} and the same element type as the values child.
 */
public class RunEndEncodedChunkReader extends BaseChunkReader<WritableChunk<Values>> {
    private static final String DEBUG_NAME = "RunEndEncodedChunkReader";

    private final ChunkReader<? extends WritableChunk<Values>> runEndsReader;
    private final ChunkReader<? extends WritableChunk<Values>> valuesReader;
    private final ChunkType valuesChunkType;

    public RunEndEncodedChunkReader(
            @NotNull final ChunkReader<? extends WritableChunk<Values>> runEndsReader,
            @NotNull final ChunkReader<? extends WritableChunk<Values>> valuesReader,
            @NotNull final ChunkType valuesChunkType) {
        this.runEndsReader = runEndsReader;
        this.valuesReader = valuesReader;
        this.valuesChunkType = valuesChunkType;
    }

    @Override
    public WritableChunk<Values> readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {

        // Consume the parent REE field node (logical length numRows, null_count 0).
        final ChunkWriter.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        // REE parent has ZERO buffers — do NOT consume any entries from bufferInfoIter here.

        final int numRows = nodeInfo.numElements;
        final WritableChunk<Values> chunk = BaseChunkReader.castOrCreateChunk(
                outChunk, outOffset, Math.max(totalRows, numRows),
                valuesChunkType::makeWritableChunk,
                c -> c);

        if (numRows == 0) {
            // Still drain the children to keep the field-node and buffer iterators aligned.
            try (final WritableChunk<Values> ignored =
                    runEndsReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0);
                    final WritableChunk<Values> ignored2 =
                            valuesReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {
                return chunk;
            }
        }

        // Read the run_ends (numRuns entries, cumulative end indices) and values (numRuns entries, one per run).
        try (final WritableChunk<Values> runEnds =
                runEndsReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0);
                final WritableChunk<Values> runValues =
                        valuesReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {

            final BarrageRunKernel kernel = BarrageRunKernel.makeBarrageRunKernel(valuesChunkType);
            final IntUnaryOperator reader = BarrageRunKernel.runEndReader(runEnds);
            kernel.decodeRunEnds(reader, runValues, chunk, outOffset);
        }

        return chunk;
    }
}
