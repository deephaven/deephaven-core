package io.deephaven.db.v2.hashing;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkPositions;

public interface ChunkEquals {
    /**
     * Returns true iff the chunks have the same size() and each corresponding element of the chunk
     * compares equal.
     *
     * @param lhs the left-hand side of the comparison
     * @param rhs the right-hand side of the comparison
     */
    boolean equalReduce(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs);

    /**
     * Called for the first (or only) pair of chunks, sets the corresponding destination entry to
     * true if the values are equal, or false otherwise
     *
     * @param lhs the left-hand side of the comparison
     * @param rhs the right-hand side of the comparison
     * @param destination the chunk to write equality values into
     */
    void equal(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs,
        WritableBooleanChunk destination);

    /**
     * For each pair of indices i and i + 1 in chunk; write true to destination[i] if they are
     * equal, otherwise write false.
     *
     * @param chunk the chunk to compare subsequent values in
     * @param destination the chunk to write equality values into, size is chunk.size() - 1
     */
    void equalNext(Chunk<? extends Any> chunk, WritableBooleanChunk destination);

    /**
     * Called for the first (or only) pair of chunks, sets the corresponding destination entry to
     * true if lhs[lhsPositions] == rhs[rhsPositions].
     *
     * @param lhsPositions the positions within left-hand side of the comparison
     * @param rhsPositions the positions within the right-hand side of the comparison
     * @param lhs the left-hand side of the comparison
     * @param rhs the right-hand side of the comparison
     * @param destination the chunk to write equality values into
     */
    void equalPermuted(IntChunk<ChunkPositions> lhsPositions, IntChunk<ChunkPositions> rhsPositions,
        Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination);

    /**
     * Called for the first (or only) pair of chunks, sets the corresponding destination entry to
     * true if lhs[lhsPositions] == rhs.
     *
     * @param lhsPositions the positions within left-hand side of the comparison
     * @param lhs the left-hand side of the comparison
     * @param rhs the right-hand side of the comparison
     * @param destination the chunk to write equality values into
     */
    void equalLhsPermuted(IntChunk<ChunkPositions> lhsPositions, Chunk<? extends Any> lhs,
        Chunk<? extends Any> rhs, WritableBooleanChunk destination);

    /**
     * Called for subsequent pair of chunks, if the corresponding destination entry is false, do
     * nothing. If true, then set to false if the corresponding values are not equal.
     *
     * @param lhs the left-hand side of the comparison
     * @param rhs the right-hand side of the comparison
     * @param destination the chunk to write equality values into
     */
    void andEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs,
        WritableBooleanChunk destination);

    /**
     * For each pair of indices i and i + 1 in chunk; if destination[i] is false do nothing,
     * otherwise write true to destination[i] if they are equal.
     *
     * @param chunk the chunk to compare subsequent values in
     * @param destination the chunk to write equality values into, size is chunk.size() - 1
     */
    void andEqualNext(Chunk<? extends Any> chunk, WritableBooleanChunk destination);

    /**
     * If destination[i] is false do nothing, otherwise, sets the corresponding destination entry to
     * true if lhs[lhsPositions] == rhs[rhsPositions].
     *
     * @param lhsPositions the positions within left-hand side of the comparison
     * @param rhsPositions the positions within the right-hand side of the comparison
     * @param lhs the left-hand side of the comparison
     * @param rhs the right-hand side of the comparison
     * @param destination the chunk to write equality values into
     */
    void andEqualPermuted(IntChunk<ChunkPositions> lhsPositions,
        IntChunk<ChunkPositions> rhsPositions, Chunk<? extends Any> lhs, Chunk<? extends Any> rhs,
        WritableBooleanChunk destination);

    /**
     * If destination[i] is false do nothing, otherwise, sets the corresponding destination entry to
     * true if lhs[lhsPositions] == rhs.
     *
     * @param lhsPositions the positions within left-hand side of the comparison
     * @param lhs the left-hand side of the comparison
     * @param rhs the right-hand side of the comparison
     * @param destination the chunk to write equality values into
     */
    void andEqualLhsPermuted(IntChunk<ChunkPositions> lhsPositions, Chunk<? extends Any> lhs,
        Chunk<? extends Any> rhs, WritableBooleanChunk destination);

    /**
     * Called for the first (or only) pair of chunks, sets the corresponding destination entry to
     * true if the values are not equal, or false otherwise
     *
     * @param lhs the left-hand side of the comparison
     * @param rhs the right-hand side of the comparison
     * @param destination the chunk to write equality values into
     */
    void notEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs,
        WritableBooleanChunk destination);

    /**
     * Called for subsequent pair of chunks, if the corresponding destination entry is false, do
     * nothing. If true, then set to false if the corresponding values are equal.
     *
     * @param lhs the left-hand side of the comparison
     * @param rhs the right-hand side of the comparison
     * @param destination the chunk to write equality values into
     */
    void andNotEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs,
        WritableBooleanChunk destination);


    /**
     * Compares valuesChunk[chunkPositionsToCheckForEquality[pp * 2]] and
     * valuesChunk[chunkPositionsToCheckForEquality[pp * 2 + 1]] for each even/odd pair in
     * chunkPositionsToCheckForEquality and writes the result to destinations.
     *
     * @param chunkPositionsToCheckForEquality the position pairs of interest
     * @param valuesChunk the chunk of values we are interested in
     * @param destinations the destination chunk to write equality values into
     */
    void equalPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality,
        Chunk<? extends Any> valuesChunk, WritableBooleanChunk destinations);

    /**
     * Compares valuesChunk[chunkPositionsToCheckForEquality[pp * 2]] and
     * valuesChunk[chunkPositionsToCheckForEquality[pp * 2 + 1]] for each even/odd pair in
     * chunkPositionsToCheckForEquality and writes the result to destinations.
     *
     * @param chunkPositionsToCheckForEquality the position pairs of interest
     * @param valuesChunk the chunk of values we are interested in
     * @param destinations the destination chunk to write equality values into
     */
    void andEqualPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality,
        Chunk<? extends Any> valuesChunk, WritableBooleanChunk destinations);

    static ChunkEquals makeEqual(ChunkType chunkType) {
        switch (chunkType) {
            case Boolean:
                return BooleanChunkEquals.INSTANCE;
            case Byte:
                return ByteChunkEquals.INSTANCE;
            case Char:
                return CharChunkEquals.INSTANCE;
            case Int:
                return IntChunkEquals.INSTANCE;
            case Short:
                return ShortChunkEquals.INSTANCE;
            case Long:
                return LongChunkEquals.INSTANCE;
            case Float:
                return FloatChunkEquals.INSTANCE;
            case Double:
                return DoubleChunkEquals.INSTANCE;
            case Object:
                return ObjectChunkEquals.INSTANCE;
        }
        throw new IllegalStateException();
    }
}
