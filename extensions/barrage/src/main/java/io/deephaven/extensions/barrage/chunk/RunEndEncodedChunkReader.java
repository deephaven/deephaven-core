//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

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

            final int numRuns = LongSizedDataStructure.intSize(DEBUG_NAME, runEnds.size());

            // Expand runs into the flat output chunk without boxing (typed dispatch per ChunkType).
            int start = 0;
            for (int runIndex = 0; runIndex < numRuns; ++runIndex) {
                final int end = getRunEnd(runEnds, runIndex);
                final int length = end - start;
                fillRunRange(chunk, outOffset + start, length, runValues, runIndex);
                start = end;
            }
        }

        return chunk;
    }

    // ---- static helpers ----

    /**
     * Read the cumulative run-end index at position {@code runIndex}, casting to {@code int}. Incoming batches are
     * always int-bounded, so run_end values never exceed {@link Integer#MAX_VALUE} regardless of the Arrow index type.
     */
    @VisibleForTesting
    static int getRunEnd(final Chunk<Values> runEnds, final int runIndex) {
        switch (runEnds.getChunkType()) {
            case Short:
                // run_ends for Int16 are always positive (writer enforces numRows <= Short.MAX_VALUE).
                return runEnds.asShortChunk().get(runIndex);
            case Int:
                return runEnds.asIntChunk().get(runIndex);
            case Long:
                return LongSizedDataStructure.intSize(DEBUG_NAME, runEnds.asLongChunk().get(runIndex));
            default:
                throw new IllegalStateException(
                        "run_ends ChunkType must be Short, Int, or Long; got: " + runEnds.getChunkType());
        }
    }

    /**
     * Fill {@code dest[destStart .. destStart+length)} with the value at {@code src[srcPos]}. Dispatches on the actual
     * chunk type to avoid boxing; handles null values correctly for both primitive (NULL sentinel) and Object (null
     * reference) chunk types.
     */
    @VisibleForTesting
    static void fillRunRange(
            final WritableChunk<Values> dest,
            final int destStart,
            final int length,
            final Chunk<Values> src,
            final int srcPos) {
        switch (dest.getChunkType()) {
            case Boolean:
                dest.asWritableBooleanChunk().fillWithValue(destStart, length, src.asBooleanChunk().get(srcPos));
                break;
            case Byte:
                dest.asWritableByteChunk().fillWithValue(destStart, length, src.asByteChunk().get(srcPos));
                break;
            case Char:
                dest.asWritableCharChunk().fillWithValue(destStart, length, src.asCharChunk().get(srcPos));
                break;
            case Short:
                dest.asWritableShortChunk().fillWithValue(destStart, length, src.asShortChunk().get(srcPos));
                break;
            case Int:
                dest.asWritableIntChunk().fillWithValue(destStart, length, src.asIntChunk().get(srcPos));
                break;
            case Long:
                dest.asWritableLongChunk().fillWithValue(destStart, length, src.asLongChunk().get(srcPos));
                break;
            case Float:
                dest.asWritableFloatChunk().fillWithValue(destStart, length, src.asFloatChunk().get(srcPos));
                break;
            case Double:
                dest.asWritableDoubleChunk().fillWithValue(destStart, length, src.asDoubleChunk().get(srcPos));
                break;
            case Object:
                dest.asWritableObjectChunk().fillWithValue(destStart, length, src.asObjectChunk().get(srcPos));
                break;
            default:
                throw new IllegalStateException("Unsupported ChunkType: " + dest.getChunkType());
        }
    }
}
