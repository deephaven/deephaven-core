package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.RangeEndRule;
import io.deephaven.api.RangeStartRule;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation.leftPositionToOutputEndPosition;
import static io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation.leftPositionToOutputStartPosition;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * {@link RangeSearchKernel} for values of type char.
 */
class RangeSearchKernelChar implements RangeSearchKernel {

    static final RangeSearchKernelChar INSTANCE = new RangeSearchKernelChar();

    private RangeSearchKernelChar() {}

    @Override
    public void populateInvalidRanges(
            @NotNull final Chunk<? extends Values> leftStartValues,
            @NotNull final Chunk<? extends Values> leftEndValues,
            final boolean allowEqual,
            @NotNull final WritableBooleanChunk<? super Values> validity,
            @NotNull final WritableIntChunk<? extends Values> output) {
        RangeSearchKernelChar.populateInvalidRanges(
                leftStartValues.asCharChunk(), leftEndValues.asCharChunk(), allowEqual,
                validity, output);
    }

    @Override
    public void findStarts(
            @NotNull final Chunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final Chunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final RangeStartRule rule,
            @NotNull final WritableIntChunk<? extends Values> output) {
        RangeSearchKernelChar.findStarts(
                leftValues.asCharChunk(), leftPositions,
                rightValues.asCharChunk(), rightStartOffsets,
                rule, output);
    }

    @Override
    public void findEnds(
            @NotNull final Chunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final Chunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final RangeEndRule rule,
            @NotNull final WritableIntChunk<? extends Values> output) {
        RangeSearchKernelChar.findEnds(
                leftValues.asCharChunk(), leftPositions,
                rightValues.asCharChunk(), rightStartOffsets, rightLengths,
                rule, output);
    }

    private static void populateInvalidRangesAllowEqual(
            @NotNull CharChunk<? extends Values> leftStartValues,
            @NotNull CharChunk<? extends Values> leftEndValues,
            @NotNull WritableBooleanChunk<? super Values> validity,
            @NotNull WritableIntChunk<? extends Values> output) {
        final int size = leftStartValues.size();
        Assert.eq(size, "leftStartValues.size()", leftEndValues.size(), "leftEndValues.size()");

        for (int li = 0; li < size; ++li) {
            if (isValidPairAllowEqual(leftStartValues.get(li), leftEndValues.get(li))) {
                validity.set(li, true);
            } else {
                validity.set(li, false);
                output.set(leftPositionToOutputStartPosition(li), NULL_INT);
                output.set(leftPositionToOutputEndPosition(li), NULL_INT);
            }
        }
    }

    private static void populateInvalidRangesDisallowEqual(
            @NotNull CharChunk<? extends Values> leftStartValues,
            @NotNull CharChunk<? extends Values> leftEndValues,
            @NotNull WritableBooleanChunk<? super Values> validity,
            @NotNull WritableIntChunk<? extends Values> output) {
        final int size = leftStartValues.size();
        Assert.eq(size, "leftStartValues.size()", leftEndValues.size(), "leftEndValues.size()");

        for (int li = 0; li < size; ++li) {
            if (isValidPairDisallowEqual(leftStartValues.get(li), leftEndValues.get(li))) {
                validity.set(li, true);
            } else {
                validity.set(li, false);
                output.set(leftPositionToOutputStartPosition(li), NULL_INT);
                output.set(leftPositionToOutputEndPosition(li), NULL_INT);
            }
        }
    }

    private static void populateAllRangesEmptyRightAllowEqual(
            @NotNull CharChunk<? extends Values> leftStartValues,
            @NotNull CharChunk<? extends Values> leftEndValues,
            @NotNull WritableIntChunk<? extends Values> output) {
        final int size = leftStartValues.size();
        Assert.eq(size, "leftStartValues.size()", leftEndValues.size(), "leftEndValues.size()");

        for (int li = 0; li < size; ++li) {
            if (isValidPairAllowEqual(leftStartValues.get(li), leftEndValues.get(li))) {
                output.set(leftPositionToOutputStartPosition(li), 0);
                output.set(leftPositionToOutputEndPosition(li), 0);
            } else {
                output.set(leftPositionToOutputStartPosition(li), NULL_INT);
                output.set(leftPositionToOutputEndPosition(li), NULL_INT);
            }
        }
    }

    private static void populateAllRangesEmptyRightDisallowEqual(
            @NotNull CharChunk<? extends Values> leftStartValues,
            @NotNull CharChunk<? extends Values> leftEndValues,
            @NotNull WritableIntChunk<? extends Values> output) {
        final int size = leftStartValues.size();
        Assert.eq(size, "leftStartValues.size()", leftEndValues.size(), "leftEndValues.size()");

        for (int li = 0; li < size; ++li) {
            if (isValidPairDisallowEqual(leftStartValues.get(li), leftEndValues.get(li))) {
                output.set(leftPositionToOutputStartPosition(li), 0);
                output.set(leftPositionToOutputEndPosition(li), 0);
            } else {
                output.set(leftPositionToOutputStartPosition(li), NULL_INT);
                output.set(leftPositionToOutputEndPosition(li), NULL_INT);
            }
        }
    }

    private static boolean isValidPairAllowEqual(final char start, final char end) {
        return !isNaN(start) && !isNaN(end) && (isNull(start) || isNull(end) || leq(start, end));
    }

    private static boolean isValidPairDisallowEqual(final char start, final char end) {
        return !isNaN(start) && !isNaN(end) && (isNull(start) || isNull(end) || lt(start, end));
    }

    private static void findStartsLessThan(
            @NotNull final CharChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final CharChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftValues(leftValues, leftPositions, output, leftSize);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        char leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point for not found, or bump by one for found to avoid equal values
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult + 1;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we reach rightValue.
            final char rightValue = rightValues.get(rightIndex);
            while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeStarts(leftPositions, rightStartOffsets, rightLengths, output, leftSize, rightSize, leftIndex);
    }

    /**
     * Process an initial sequence of null start values, which cause the range to start from right position 0
     * 
     * @param leftValues The left values, sorted according to Deephaven sorting order (nulls first)
     * @param leftPositions The left positions, parallel to {@code leftValues}, used to determine {@code output} index
     * @param output The output chunk
     * @param leftSize The size of {@code leftValues} and {@code leftPositions}, for convenience
     * @return The number of left indices consumed
     */
    private static int consumeNullLeftValues(
            @NotNull final CharChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final WritableIntChunk<? extends Values> output,
            final int leftSize) {
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), 0);
        }
        return leftIndex;
    }

    /**
     * Fill range starts for left start values with no responsive right values.
     *
     * @param leftPositions The left positions, used to determine {@code output} index
     * @param rightStartOffsets The right run start offsets
     * @param rightLengths The right run lengths
     * @param output The output chunk
     * @param leftSize The size of {@code leftPositions}
     * @param rightSize The size of {@code rightStartOffsets} and {@code rightLengths}
     * @param leftIndex The left index to start from
     */
    private static void consumeEmptyRangeStarts(
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output,
            final int leftSize,
            final int rightSize,
            int leftIndex) {
        if (leftIndex == leftSize) {
            return;
        }
        final int rightLastPositionExclusive = rightStartOffsets.get(rightSize - 1) + rightLengths.get(rightSize - 1);
        while (leftIndex < leftSize) {
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightLastPositionExclusive);
        }
    }

    private static void findStartsLessThanEqual(
            @NotNull final CharChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final CharChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        // Process an initial sequence of null start values, which cause the range to start from right position 0
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), 0);
        }
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        char leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point in all cases
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we pass rightValue.
            final char rightValue = rightValues.get(rightIndex);
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        if (leftIndex == leftSize) {
            return;
        }
        // Fill range starts for left values after the last right value
        final int rightLastPositionExclusive = rightStartOffsets.get(rightSize - 1) + rightLengths.get(rightSize - 1);
        while (leftIndex < leftSize) {
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightLastPositionExclusive);
        }
    }

    private static void findStartsLessThanEqualAllowPreceding(
            @NotNull final CharChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final CharChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        // Process an initial sequence of null start values, which cause the range to start from right position 0
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), 0);
        }
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        char leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            final int rightIndex;
            final int precedingRightPosition;
            final int rightPosition;
            final char rightValue;
            if (searchResult == -1) {
                // Insertion point is 0. We can't look back from there, so we use the insertion point.
                rightIndex = 0;
                precedingRightPosition = -1;
                rightPosition = rightStartOffsets.get(0);
                rightValue = rightValues.get(0);
            } else if (searchResult < 0) {
                // Insertion point is not an exact match, so look behind
                rightIndex = ~searchResult;
                precedingRightPosition = rightStartOffsets.get(rightIndex - 1) + rightLengths.get(rightIndex - 1) - 1;
                rightPosition = rightStartOffsets.get(rightIndex);
                rightValue = rightValues.get(rightIndex);
            } else {
                // We found an exact match, so use it
                rightIndex = searchResult;
                precedingRightPosition = -1;
                rightPosition = rightStartOffsets.get(searchResult);
                rightValue = rightValues.get(searchResult);
            }
            // Proceed linearly until we have a reason to binary search again
            if (precedingRightPosition < 0) {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            } else {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), precedingRightPosition);
                // We can re-use precedingRightPosition until we reach rightValue
                while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                    output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), precedingRightPosition);
                }
            }
            // We can re-use rightPosition until we pass rightValue
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        if (leftIndex == leftSize) {
            return;
        }
        // Fill range starts for left values after the last right value
        final int rightLastPositionExclusive = rightStartOffsets.get(rightSize - 1) + rightLengths.get(rightSize - 1);
        while (leftIndex < leftSize) {
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightLastPositionExclusive);
        }
    }

    private static void findEnds(
            @NotNull final CharChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final CharChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final RangeEndRule rule,
            @NotNull final WritableIntChunk<? extends Values> output) {
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        if (rightSize == 0) {
            for (int li = 0; li < leftSize; ++li) {
                output.set(leftPositionToOutputEndPosition(leftPositions.get(li)), 0);
            }
            return;
        }
    }

    private static boolean isNaN(final char c) {
        // region isNaN
        return false;
        // endregion isNaN
    }

    private static boolean isNull(final char c) {
        // We need not deal with NaN values here
        // region isNull
        return c == NULL_CHAR;
        // endregion isNull
    }

    private static boolean lt(final char a, final char b) {
        // We need not deal with null or NaN values here
        // region lt
        return a < b;
        // endregion lt
    }

    private static boolean leq(final char a, final char b) {
        // We need not deal with null or NaN values here
        // region lt
        return a <= b;
        // endregion lt
    }

    private static boolean gt(final char a, final char b) {
        // We need not deal with null or NaN values here
        // region lt
        return a > b;
        // endregion lt
    }

    private static boolean geq(final char a, final char b) {
        // We need not deal with null or NaN values here
        // region geq
        return a >= b;
        // endregion geq
    }
}
