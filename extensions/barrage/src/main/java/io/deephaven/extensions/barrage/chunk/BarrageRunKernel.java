//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import java.util.function.IntConsumer;

/**
 * Typed kernel for single-pass run detection over a row subset, used by {@link RunEndEncodedChunkWriter}. Each
 * implementation is specialized for one value chunk type, avoiding virtual dispatch on per-element reads and writes.
 */
public interface BarrageRunKernel {

    static BarrageRunKernel makeBarrageRunKernel(final ChunkType valuesChunkType) {
        switch (valuesChunkType) {
            case Char:
                return CharBarrageRunKernel.INSTANCE;
            case Byte:
                return ByteBarrageRunKernel.INSTANCE;
            case Short:
                return ShortBarrageRunKernel.INSTANCE;
            case Int:
                return IntBarrageRunKernel.INSTANCE;
            case Long:
                return LongBarrageRunKernel.INSTANCE;
            case Float:
                return FloatBarrageRunKernel.INSTANCE;
            case Double:
                return DoubleBarrageRunKernel.INSTANCE;
            default:
                return ObjectBarrageRunKernel.INSTANCE;
        }
    }

    /**
     * Single-pass run detection directly over the subset. Reads {@code src} at the row positions given by
     * {@code subset}, detects runs and stores runEnds and runValues into the provided {@link WritableChunk chunks}.
     * <p>
     * The caller must pre-allocate {@code runEnds} and {@code runValues} to capacity {@code subset.intSize()}
     * (worst-case scenario)
     */
    void computeRuns(
            Chunk<Values> src,
            RowSequence subset,
            WritableChunk<Values> runEnds,
            WritableChunk<Values> runValues);

    /**
     * Create a type-specialized {@link IntConsumer} that appends to {@code runEnds}.
     */
    static IntConsumer runEndAdder(final WritableChunk<Values> runEnds) {
        switch (runEnds.getChunkType()) {
            case Short: {
                final var c = runEnds.asWritableShortChunk();
                return v -> c.add((short) v);
            }
            case Int: {
                final var c = runEnds.asWritableIntChunk();
                return c::add;
            }
            case Long: {
                final var c = runEnds.asWritableLongChunk();
                return c::add;
            }
            default:
                throw new IllegalStateException(
                        "Unexpected run_ends ChunkType: " + runEnds.getChunkType());
        }
    }
}
