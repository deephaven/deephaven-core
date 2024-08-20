//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.RangeEndRule;
import io.deephaven.api.RangeStartRule;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

/**
 * Kernel interface for finding range start and end positions for range join.
 */
interface RangeSearchKernel {

    /**
     * Lookup the appropriate singleton RangeSearchKernel for the supplied chunk type and rules.
     *
     * @param chunkType The chunk type for the values to be searched
     * @param startRule The range start rule
     * @param endRule The range end rule
     * @return The RangeSearchKernel to use
     */
    static RangeSearchKernel makeRangeSearchKernel(
            @NotNull final ChunkType chunkType,
            @NotNull final RangeStartRule startRule,
            @NotNull final RangeEndRule endRule) {
        switch (chunkType) {
            case Boolean:
                throw new UnsupportedOperationException("Cannot create a range search kernel for primitive booleans");
            case Char:
                return RangeSearchKernelChar.forRules(startRule, endRule);
            case Byte:
                return RangeSearchKernelByte.forRules(startRule, endRule);
            case Short:
                return RangeSearchKernelShort.forRules(startRule, endRule);
            case Int:
                return RangeSearchKernelInt.forRules(startRule, endRule);
            case Long:
                return RangeSearchKernelLong.forRules(startRule, endRule);
            case Float:
                return RangeSearchKernelFloat.forRules(startRule, endRule);
            case Double:
                return RangeSearchKernelDouble.forRules(startRule, endRule);
            case Object:
                return RangeSearchKernelObject.forRules(startRule, endRule);
            default:
                throw new UnsupportedOperationException(String.format(
                        "Cannot create a range search kernel for unknown chunk type %s", chunkType));
        }
    }

    /**
     * Examine the (source-ordered) left start and end value pairs in {@code leftStartValues} and {@code leftEndValues},
     * populating range start positions and range end positions in {@code output} for all pairs. Valid pairs will have
     * an empty result, while invalid pairs will have a {@code null} result. This method also ensures that the output
     * chunks have their size set to match {@code leftStartValues.size()}, which must match
     * {@code leftEndValues.size()}.
     *
     * @param leftStartValues The left start values in source (and output) order
     * @param leftEndValues The left end values in source (and output) order
     * @param outputStartPositionsInclusive The output chunk of start positions to be populated for invalid ranges
     * @param outputEndPositionsExclusive The output chunk of end positions to be populated for invalid ranges
     */
    void processAllRangesForEmptyRight(
            @NotNull Chunk<? extends Values> leftStartValues,
            @NotNull Chunk<? extends Values> leftEndValues,
            @NotNull WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull WritableIntChunk<? extends Values> outputEndPositionsExclusive);

    /**
     * Examine the (source-ordered) left start and end value pairs in {@code leftStartValues} and {@code leftEndValues},
     * populating range start positions and range end positions in {@code output} for any invalid pairs, and recording
     * whether each pair was valid in {@code validity}. This method also ensures that the output chunks have their size
     * set to match {@code leftStartValues.size()}, which must match {@code leftEndValues.size()}.
     *
     * @param leftStartValues The left start values in source (and output) order
     * @param leftEndValues The left end values in source (and output) order
     * @param validity Output chunk to record the validity of each left (start, end) pair
     * @param outputStartPositionsInclusive The output chunk of start positions to be populated for invalid ranges
     * @param outputEndPositionsExclusive The output chunk of end positions to be populated for invalid ranges
     */
    void processInvalidRanges(
            @NotNull Chunk<? extends Values> leftStartValues,
            @NotNull Chunk<? extends Values> leftEndValues,
            @NotNull WritableBooleanChunk<? super Values> validity,
            @NotNull WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull WritableIntChunk<? extends Values> outputEndPositionsExclusive);

    /**
     * Find the appropriate right range start position (inclusive) for each left value, and record it at the appropriate
     * output slot.
     *
     * @param leftValues Ascending left start values
     * @param leftPositions Left positions, sorted according to {@code leftValues}, to be used when writing to
     *        {@code outputStartPositionsInclusive}
     * @param rightValues Ascending, de-duplicated right values
     * @param rightStartOffsets Right run start offsets corresponding to each element in {@code rightValues}
     * @param rightSizeExpanded Total size of right, before de-duplication
     * @param outputStartPositionsInclusive The output chunk of start positions
     */
    void processRangeStarts(
            @NotNull Chunk<? extends Values> leftValues,
            @NotNull IntChunk<ChunkPositions> leftPositions,
            @NotNull Chunk<? extends Values> rightValues,
            @NotNull IntChunk<ChunkPositions> rightStartOffsets,
            int rightSizeExpanded,
            @NotNull WritableIntChunk<? extends Values> outputStartPositionsInclusive);

    /**
     * Find the appropriate right range end position (exclusive) for each left value, and record it at the appropriate
     * output slot.
     *
     * @param leftValues Ascending left end values
     * @param leftPositions Left positions, sorted according to {@code leftValues}, to be used when writing to
     *        {@code outputEndPositionsExclusive}
     * @param rightValues Ascending, de-duplicated right values
     * @param rightStartOffsets Right run start offsets corresponding to each element in {@code rightValues}
     * @param rightSizeExpanded Total size of right, before de-duplication
     * @param outputEndPositionsExclusive The output chunk of end positions
     */
    void processRangeEnds(
            @NotNull Chunk<? extends Values> leftValues,
            @NotNull IntChunk<ChunkPositions> leftPositions,
            @NotNull Chunk<? extends Values> rightValues,
            @NotNull IntChunk<ChunkPositions> rightStartOffsets,
            int rightSizeExpanded,
            @NotNull WritableIntChunk<? extends Values> outputEndPositionsExclusive);
}
