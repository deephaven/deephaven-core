package io.deephaven.engine.table.impl.sort.findruns;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.Context;
import io.deephaven.chunk.*;

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
     *
     * Runs with only a single value are not included.
     *
     * @param sortedValues a chunk of sorted values
     * @param offsetsOut an output chunk, with offsets of starting locations that a run occurred
     * @param lengthsOut an output chunk, parallel to offsetsOut, with the lengths of found runs
     */
    void findRuns(Chunk sortedValues, WritableIntChunk<ChunkPositions> offsetsOut,
            WritableIntChunk<ChunkLengths> lengthsOut);

    /**
     * Find runs of identical values in a sorted chunk.
     *
     * Runs of a single values are included.
     *
     * @param sortedValues a chunk of sorted values
     * @param offsetsOut an output chunk, with offsets of starting locations that a run occurred
     * @param lengthsOut an output chunk, parallel to offsetsOut, with the lengths of found runs
     */
    void findRunsSingles(Chunk sortedValues, WritableIntChunk<ChunkPositions> offsetsOut,
            WritableIntChunk<ChunkLengths> lengthsOut);

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
    void findRuns(Chunk sortedValues, IntChunk<ChunkPositions> offsetsIn, IntChunk<ChunkLengths> lengthsIn,
            WritableIntChunk<ChunkPositions> offsetsOut, WritableIntChunk<ChunkLengths> lengthsOut);
}
