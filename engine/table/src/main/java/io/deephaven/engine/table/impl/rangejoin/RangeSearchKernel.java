package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.RangeEndRule;
import io.deephaven.api.RangeStartRule;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

/**
 * Kernel interface for finding range start and end positions for range join.
 */
interface RangeSearchKernel {

    /**
     * Examine the (source-ordered) left start and end value pairs in {@code leftStartValues} and {@code leftEndValues},
     * populating range start positions and range end positions in {@code output} for any invalid pairs, and recording
     * whether each pair was valid in {@code validity}.
     *
     * @param leftStartValues The left start values
     * @param leftEndValues The left end values
     * @param allowEqual Whether equal start and end values are valid
     * @param validity Output chunk to record the validity of each left (start, end) pair
     * @param output The output chunk to be populated for invalid ranges
     */
    void populateInvalidRanges(
            @NotNull Chunk<? extends Values> leftStartValues,
            @NotNull Chunk<? extends Values> leftEndValues,
            boolean allowEqual,
            @NotNull WritableBooleanChunk<? super Values> validity,
            @NotNull WritableIntChunk<? extends Values> output);

    /**
     * Find the appropriate right range start position (inclusive) for each left value, and record it at the appropriate
     * output slot.
     *
     * @param leftValues Ascending left values
     * @param leftPositions Left positions, sorted according to {@code leftValues}
     * @param rightValues Ascending, de-duplicated right values
     * @param rightStartOffsets Right run start offsets corresponding to each element in {@code rightValues}
     * @param rule The {@link RangeStartRule} that defines an appropriate range start
     * @param output The output chunk; results will be written at the appropriate multiple for start positions
     */
    void findStarts(
            @NotNull Chunk<? extends Values> leftValues,
            @NotNull IntChunk<ChunkPositions> leftPositions,
            @NotNull Chunk<? extends Values> rightValues,
            @NotNull IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull RangeStartRule rule,
            @NotNull WritableIntChunk<? extends Values> output);

    /**
     * Find the appropriate right range end position (exclusive) for each left value, and record it at the appropriate
     * output slot.
     *
     * @param leftValues Ascending left values
     * @param leftPositions Left positions, sorted according to {@code leftValues}
     * @param rightValues Ascending, de-duplicated right values
     * @param rightStartOffsets Right run start offsets corresponding to each element in {@code rightValues}
     * @param rightLengths Right run lengths corresponding to each element in {@code rightValues}
     * @param rule The {@link RangeEndRule} that defines an appropriate range end
     * @param output The output chunk; results will be written at the appropriate multiple for end positions
     */
    void findEnds(
            @NotNull Chunk<? extends Values> leftValues,
            @NotNull IntChunk<ChunkPositions> leftPositions,
            @NotNull Chunk<? extends Values> rightValues,
            @NotNull IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull IntChunk<ChunkLengths> rightLengths,
            @NotNull RangeEndRule rule,
            @NotNull WritableIntChunk<? extends Values> output);
}
