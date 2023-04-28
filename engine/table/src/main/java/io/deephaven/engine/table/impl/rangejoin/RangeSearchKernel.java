package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.RangeEndRule;
import io.deephaven.api.RangeStartRule;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.SimpleTypeMap;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiFunction;

/**
 * Kernel interface for finding range start and end positions for range join.
 */
interface RangeSearchKernel {

    /**
     * Lookup the appropriate singleton RangeSearchKernel for the supplied value type and rules.
     *
     * @param valueType The type of the values to be searched
     * @param startRule The range start rule
     * @param endRule The range end rule
     * @return The RangeSearchKernel to use
     */
    static RangeSearchKernel lookup(
            @NotNull final Class<?> valueType,
            @NotNull final RangeStartRule startRule,
            @NotNull final RangeEndRule endRule) {
        return FactoryHelper.TYPE_TO_FACTORY.get(valueType).apply(startRule, endRule);
    }

    final class FactoryHelper {

        private static final SimpleTypeMap<BiFunction<RangeStartRule, RangeEndRule, ? extends RangeSearchKernel>> TYPE_TO_FACTORY =
                SimpleTypeMap.create(
                        (final RangeStartRule start, final RangeEndRule end) -> {
                            throw new UnsupportedOperationException(
                                    "Cannot create a range search kernel for primitive booleans");
                        },
                        RangeSearchKernelChar::forRules,
                        RangeSearchKernelByte::forRules,
                        RangeSearchKernelShort::forRules,
                        RangeSearchKernelInt::forRules,
                        RangeSearchKernelLong::forRules,
                        RangeSearchKernelFloat::forRules,
                        RangeSearchKernelDouble::forRules,
                        RangeSearchKernelObject::forRules);

        private FactoryHelper() {}
    }

    /**
     * Examine the (source-ordered) left start and end value pairs in {@code leftStartValues} and {@code leftEndValues},
     * populating range start positions and range end positions in {@code output} for all pairs. Valid pairs will have
     * an empty result, while invalid pairs will have a {@code null} result.
     *
     * @param leftStartValues The left start values
     * @param leftEndValues The left end values
     * @param output The output chunk to be populated for invalid ranges
     */
    void populateAllRangeForEmptyRight(
            @NotNull Chunk<? extends Values> leftStartValues,
            @NotNull Chunk<? extends Values> leftEndValues,
            @NotNull WritableIntChunk<? extends Values> output);

    /**
     * Examine the (source-ordered) left start and end value pairs in {@code leftStartValues} and {@code leftEndValues},
     * populating range start positions and range end positions in {@code output} for any invalid pairs, and recording
     * whether each pair was valid in {@code validity}.
     *
     * @param leftStartValues The left start values
     * @param leftEndValues The left end values
     * @param validity Output chunk to record the validity of each left (start, end) pair
     * @param output The output chunk to be populated for invalid ranges
     */
    void populateInvalidRanges(
            @NotNull Chunk<? extends Values> leftStartValues,
            @NotNull Chunk<? extends Values> leftEndValues,
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
     * @param rightLengths Right run lengths corresponding to each element in {@code rightValues}
     * @param output The output chunk; results will be written at the appropriate multiple for start positions
     */
    void findStarts(
            @NotNull Chunk<? extends Values> leftValues,
            @NotNull IntChunk<ChunkPositions> leftPositions,
            @NotNull Chunk<? extends Values> rightValues,
            @NotNull IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull IntChunk<ChunkLengths> rightLengths,
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
     * @param output The output chunk; results will be written at the appropriate multiple for end positions
     */
    void findEnds(
            @NotNull Chunk<? extends Values> leftValues,
            @NotNull IntChunk<ChunkPositions> leftPositions,
            @NotNull Chunk<? extends Values> rightValues,
            @NotNull IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull IntChunk<ChunkLengths> rightLengths,
            @NotNull WritableIntChunk<? extends Values> output);
}
