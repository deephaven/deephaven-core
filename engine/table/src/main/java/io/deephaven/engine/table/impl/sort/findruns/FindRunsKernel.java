/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sort.findruns;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.Context;
import io.deephaven.chunk.*;
import org.jetbrains.annotations.NotNull;

public interface FindRunsKernel extends Context {
    static FindRunsKernel makeContext(ChunkType chunkType) {
        switch (chunkType) {
            case Char:
                return CharFindRunsKernel.createContext();
            case Byte:
                return ByteFindRunsKernel.createContext();
            case Short:
                return ShortFindRunsKernel.createContext();
            case Int:
                return IntFindRunsKernel.createContext();
            case Long:
                return LongFindRunsKernel.createContext();
            case Float:
                return FloatFindRunsKernel.createContext();
            case Double:
                return DoubleFindRunsKernel.createContext();
            default:
                return ObjectFindRunsKernel.createContext();
        }
    }

    /**
     * Find runs of identical values in a sorted chunk. This is used as part of an overall sort, after the timsort (or
     * other sorting) kernel to identify the runs that must be sorted according to secondary keys.
     * <p>
     * Runs with only a single value are not included.
     *
     * @param sortedValues a chunk of sorted values
     * @param offsetsOut an output chunk, with offsets of starting locations that a run occurred
     * @param lengthsOut an output chunk, parallel to offsetsOut, with the lengths of found runs
     */
    void findRuns(
            @NotNull Chunk<? extends Any> sortedValues,
            @NotNull WritableIntChunk<ChunkPositions> offsetsOut,
            @NotNull WritableIntChunk<ChunkLengths> lengthsOut);

    /**
     * Find runs of identical values in a sorted chunk.
     * <p>
     * Runs of a single values are included.
     *
     * @param sortedValues a chunk of sorted values
     * @param offsetsOut an output chunk, with offsets of starting locations that a run occurred
     * @param lengthsOut an output chunk, parallel to offsetsOut, with the lengths of found runs
     */
    void findRunsSingles(
            @NotNull Chunk<? extends Any> sortedValues,
            @NotNull WritableIntChunk<ChunkPositions> offsetsOut,
            @NotNull WritableIntChunk<ChunkLengths> lengthsOut);

    /**
     * Find runs of identical values in a sorted chunk. This is used as part of an overall sort, after the timsort (or
     * other sorting) kernel to identify the runs that must be sorted according to secondary keys.
     *
     * @param sortedValues a chunk of sorted values
     * @param offsetsIn the offsets within the chunk to check for runs
     * @param lengthsIn the lengths parallel to offsetsIn for run checking
     * @param offsetsOut an output chunk, with offsets of starting locations that a run occurred
     * @param lengthsOut an output chunk, parallel to offsetsOut, with the lengths of found runs
     *
     *        Note, that lengthsIn must contain values greater than 1, and lengthsOut additionally only contain values
     *        greater than one
     */
    void findRuns(
            @NotNull Chunk<? extends Any> sortedValues,
            @NotNull IntChunk<ChunkPositions> offsetsIn,
            @NotNull IntChunk<ChunkLengths> lengthsIn,
            @NotNull WritableIntChunk<ChunkPositions> offsetsOut,
            @NotNull WritableIntChunk<ChunkLengths> lengthsOut);

    /**
     * Compact already-found runs in {@code sortedValues} according to the offsets in {@code offsetsIn}.
     * <p>
     * Additionally, verify that the elements are properly ordered; returning the first position of an out-of-order
     * element.
     *
     * @param sortedValues The chunk of runs to compact
     * @param offsetsIn The start offsets for each run in {@code sortedValues}
     * @return The first position of an out-of-order element, or -1 if all elements are in order
     */
    int compactRuns(
            @NotNull WritableChunk<? extends Any> sortedValues,
            @NotNull IntChunk<ChunkPositions> offsetsIn);
}
