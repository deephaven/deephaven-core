//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

/**
 * Copies runs of values from an array to a chunk, typed once per batch of runs rather than once per run.
 */
interface RunCopyKernel {
    static RunCopyKernel makeRunCopyKernel(@NotNull final ChunkType chunkType) {
        return switch (chunkType) {
            case Char -> CharRunCopyKernel.INSTANCE;
            case Byte -> ByteRunCopyKernel.INSTANCE;
            case Short -> ShortRunCopyKernel.INSTANCE;
            case Int -> IntRunCopyKernel.INSTANCE;
            case Long -> LongRunCopyKernel.INSTANCE;
            case Float -> FloatRunCopyKernel.INSTANCE;
            case Double -> DoubleRunCopyKernel.INSTANCE;
            case Object -> ObjectRunCopyKernel.INSTANCE;
            default -> throw new IllegalArgumentException("Unsupported chunk type: " + chunkType);
        };
    }

    /**
     * @param source An array of {@code destination}'s element type
     * @param runs {@code (sourcePosition, destinationPosition, length)} triples
     */
    void copyRuns(@NotNull Object source, @NotNull IntChunk<? extends Any> runs, @NotNull WritableChunk<?> destination);
}
