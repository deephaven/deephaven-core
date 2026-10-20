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
        switch (chunkType) {
            case Char:
                return CharRunCopyKernel.INSTANCE;
            case Byte:
                return ByteRunCopyKernel.INSTANCE;
            case Short:
                return ShortRunCopyKernel.INSTANCE;
            case Int:
                return IntRunCopyKernel.INSTANCE;
            case Long:
                return LongRunCopyKernel.INSTANCE;
            case Float:
                return FloatRunCopyKernel.INSTANCE;
            case Double:
                return DoubleRunCopyKernel.INSTANCE;
            case Object:
                return ObjectRunCopyKernel.INSTANCE;
            default:
                throw new IllegalArgumentException("Unsupported chunk type: " + chunkType);
        }
    }

    /**
     * @param source An array of {@code destination}'s element type
     * @param runs {@code (sourcePosition, destinationPosition, length)} triples
     */
    void copyRuns(@NotNull Object source, @NotNull IntChunk<? extends Any> runs, @NotNull WritableChunk<?> destination);
}
