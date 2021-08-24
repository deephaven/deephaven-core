package io.deephaven.db.v2.sort.permute;

import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;

import static io.deephaven.db.v2.sources.chunk.Attributes.Any;
import static io.deephaven.db.v2.sources.chunk.Attributes.ChunkPositions;

public interface PermuteKernel {
    static PermuteKernel makePermuteKernel(ChunkType chunkType) {
        switch (chunkType) {
            case Char:
                return CharPermuteKernel.INSTANCE;
            case Byte:
                return BytePermuteKernel.INSTANCE;
            case Short:
                return ShortPermuteKernel.INSTANCE;
            case Int:
                return IntPermuteKernel.INSTANCE;
            case Long:
                return LongPermuteKernel.INSTANCE;
            case Float:
                return FloatPermuteKernel.INSTANCE;
            case Double:
                return DoublePermuteKernel.INSTANCE;
            default:
                return ObjectPermuteKernel.INSTANCE;
        }
    }

    /**
     * Permute the inputValues into outputValues according to the positions in outputPositions.
     *
     * @param inputValues a chunk of values, which must have the same size as outputPositions
     * @param outputPositions a chunk of positions, parallel to inputValues, that indicates the
     *        position in outputValues for the corresponding inputValues value
     * @param outputValues an output chunk, which must be at least as big as the largest value in
     *        outputPositions
     */
    <T extends Any> void permute(Chunk<? extends T> inputValues,
        IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues);

    /**
     * Permute the inputValues into outputValues according to positions in inputPositions and
     * outputPositions.
     * <p>
     * outputValues[outputPositions] = inputValues[inputPositions]
     *
     * @param inputPositions a chunk of positions that indicates the position in inputValues to copy
     *        to the outputValues chunk
     * @param inputValues a chunk of values, which must be at least as large as the largest value in
     *        inputPositions
     * @param outputPositions a chunk of positions, parallel to inputPositions, that indicates the
     *        position in outputValues for the corresponding inputValues value
     * @param outputValues an output chunk, which must be at least as big as the largest value in
     *        outputPositions
     */
    <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions,
        Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions,
        WritableChunk<? super T> outputValues);

    /**
     * Permute the inputValues into outputValues according to the positions in inputPositions.
     *
     * @param inputValues a chunk of values, which must be at least as big as the largest value in
     *        inputPositions
     * @param inputPositions a chunk of positions, parallel to outputValues, that indicates the
     *        position in inputValues for the corresponding outputValues value
     * @param outputValues an output chunk, which must have the same size as inputPositions
     */
    <T extends Any> void permuteInput(Chunk<? extends T> inputValues,
        IntChunk<ChunkPositions> inputPositions, WritableChunk<? super T> outputValues);
}
