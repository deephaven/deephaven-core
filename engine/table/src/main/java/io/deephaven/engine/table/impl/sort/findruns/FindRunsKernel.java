//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.findruns;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.*;
import io.deephaven.base.verify.Assert;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

public interface FindRunsKernel {

    static FindRunsKernel getInstance(ChunkType chunkType) {
        switch (chunkType) {
            case Char:
                return CharFindRunsKernel.getInstance();
            case Byte:
                return ByteFindRunsKernel.getInstance();
            case Short:
                return ShortFindRunsKernel.getInstance();
            case Int:
                return IntFindRunsKernel.getInstance();
            case Long:
                return LongFindRunsKernel.getInstance();
            case Float:
                return FloatFindRunsKernel.getInstance();
            case Double:
                return DoubleFindRunsKernel.getInstance();
            default:
                return ObjectFindRunsKernel.getInstance();
        }
    }

    /**
     * Get a kernel that finds runs of values that compare equal under {@code comparator}, or under the natural ordering
     * of {@code chunkType} when {@code comparator} is null.
     *
     * @param chunkType the type of the sorted chunks
     * @param comparator the comparator the values were sorted with, or null for the natural ordering
     * @return a kernel whose runs are the ties of a sort with the same ordering
     */
    static FindRunsKernel getInstance(ChunkType chunkType, Comparator<?> comparator) {
        if (comparator == null) {
            return getInstance(chunkType);
        }
        Assert.eq(chunkType, "chunkType", ChunkType.Object, "ChunkType.Object");
        return new ObjectComparatorFindRunsKernel(comparator);
    }

    /**
     * Find runs of values that compare equal in a sorted chunk. This is used as part of an overall sort, after the
     * timsort (or other sorting) kernel to identify the runs that must be sorted according to secondary keys.
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
     * Find runs of values that compare equal in a sorted chunk.
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
     * Find runs of values that compare equal in a sorted chunk. This is used as part of an overall sort, after the
     * timsort (or other sorting) kernel to identify the runs that must be sorted according to secondary keys.
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
