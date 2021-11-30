package io.deephaven.engine.table.impl.util.compact;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;

public interface CompactKernel {
    /**
     * Compacts values into the front of the chunk, retaining only values where the parallel retainValues chunk has a
     * true value.
     *
     * @param values a chunk of values, input and output
     * @param retainValues the values to retain
     */
    void compact(WritableChunk<? extends Any> values, BooleanChunk<Any> retainValues);

    /**
     * Sort valuesChunk, eliminate duplicates, and write the number of times a value occurred into the parallel slot
     * within counts. null values are removed from the chunk.
     *
     * @param valueChunk a chunk of values, input and output
     * @param counts an output chunk parallel to valueChunk with the number of times a value occurred
     */
    default void compactAndCount(WritableChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts) {
        compactAndCount(valueChunk, counts, false);
    }

    /**
     * Sort valuesChunk, eliminate duplicates, and write the number of times a value occurred into the parallel slot
     * within counts.
     *
     * @param valueChunk a chunk of values, input and output
     * @param counts an output chunk parallel to valueChunk with the number of times a value occurred
     * @param countNull if the compaction should count nulls or not
     */
    void compactAndCount(WritableChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts, boolean countNull);

    /**
     * For each run in valuesChunk, sort it, eliminate duplicates, and write the number of times a value occurred into
     * the parallel slot within counts. null values are removed from the chunk.
     *
     * @param valueChunk a chunk of values, input and output
     * @param counts an output chunk parallel to valueChunk with the number of times a value occurred
     * @param startPositions the start of each run
     * @param lengths the length of each run, input and output
     */
    default void compactAndCount(WritableChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts, IntChunk<ChunkPositions> startPositions,
            WritableIntChunk<ChunkLengths> lengths) {
        compactAndCount(valueChunk, counts, startPositions, lengths, false);
    }

    /**
     * For each run in valuesChunk, sort it, eliminate duplicates, and write the number of times a value occurred into
     * the parallel slot within counts.
     *
     * @param valueChunk a chunk of values, input and output
     * @param counts an output chunk parallel to valueChunk with the number of times a value occurred
     * @param startPositions the start of each run
     * @param lengths the length of each run, input and output
     * @param countNull if the compaction should count nulls or not
     */
    void compactAndCount(WritableChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts, IntChunk<ChunkPositions> startPositions,
            WritableIntChunk<ChunkLengths> lengths, boolean countNull);

    static CompactKernel makeCompact(ChunkType chunkType) {
        switch (chunkType) {
            case Boolean:
                return BooleanCompactKernel.INSTANCE;
            case Char:
                return CharCompactKernel.INSTANCE;
            case Byte:
                return ByteCompactKernel.INSTANCE;
            case Short:
                return ShortCompactKernel.INSTANCE;
            case Int:
                return IntCompactKernel.INSTANCE;
            case Long:
                return LongCompactKernel.INSTANCE;
            case Float:
                return FloatCompactKernel.INSTANCE;
            case Double:
                return DoubleCompactKernel.INSTANCE;
            case Object:
                return ObjectCompactKernel.INSTANCE;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
