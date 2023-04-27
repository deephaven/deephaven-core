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

    private static void findStarts(
            @NotNull final CharChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final CharChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final RangeStartRule rule,
            @NotNull final WritableIntChunk<? extends Values> output) {
        final int leftSize = leftValues.size();
        final int leftValidSize;

        // First, handle a trailing region of NaN left values (for floating point types)
        // region NaN-handling-for-starts
        leftValidSize = leftSize;
        // endregion NaN-handling-for-starts

        // Next, handle the case where there are no right rows, by recording that all valid ranges begin at 0
        final int rightSize = rightValues.size();
        if (rightSize == 0) {
            for (int leftIndex = 0; leftIndex < leftValidSize; ++leftIndex) {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex)), 0);
            }
            return;
        }

        int leftIndex = 0;
        for (; leftIndex < leftValidSize && isNull(leftValues.get(leftIndex)); ++leftIndex) {
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex)), 0);
        }

    }

    private static void findStartsLessThan(
            @NotNull final CharChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final CharChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final WritableIntChunk<? extends Values> output,
            int nextLeftIndex,
            final int leftValidSize) {
        // First, handle a trailing region of NaN left values (for floating point types)
        // region NaN-handling-for-starts
        leftValidSize = leftSize;
        // endregion NaN-handling-for-starts

        // Next, handle the case where there are no right rows, by recording that all valid ranges begin at 0
        final int rightSize = rightValues.size();
        if (rightSize == 0) {
            for (int leftIndex = 0; leftIndex < leftValidSize; ++leftIndex) {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex)), 0);
            }
            return;
        }

        int leftIndex = 0;
        for (; leftIndex < leftValidSize && isNull(leftValues.get(leftIndex)); ++leftIndex) {
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex)), 0);
        }

        int rightHighIndex = rightSize - 1;
        char rightHighValue = rightValues.get(rightHighIndex);
        for (int li = 0; li < leftSize;) {
            final char leftValue = leftValues.get(li);
            final int outputPosition = leftPositionToOutputStartPosition(leftPositions.get(li));

            if (geq(leftValue, rightHighValue)) {
                break;
            }

            int rightLowIndex = 0;
            while (rightLowIndex < rightHighIndex) {
                final int rightMidIndex = ((rightHighIndex - rightLowIndex) / 2) + rightLowIndex;
                final char rightMidValue = rightValues.get(rightMidIndex);

                if (lt(leftValue, rightMidValue)) {
                    rightHighIndex = rightMidIndex;
                    if (rightLowIndex == rightHighIndex - 1) {
                        break;
                    }
                } else {
                    rightLowIndex = rightMidIndex;
                }
            }

            final long redirectionKey = rightKeyIndices.get(rightLowIdx);
            if (rightLowIdx == maxRightIdx) {
                leftRedirections.fillWithValue(li, leftSize - li, redirectionKey);
                return;
            } else {
                leftRedirections.set(li++, redirectionKey);
                final char nextRightValue = rightStamps.get(rightLowIdx + 1);
                while (li < leftSize && lt(leftStamps.get(li), nextRightValue)) {
                    leftRedirections.set(li++, redirectionKey);
                }
            }
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
