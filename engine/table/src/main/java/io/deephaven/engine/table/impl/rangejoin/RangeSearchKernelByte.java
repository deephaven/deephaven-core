/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit RangeSearchKernelChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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
import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * {@link RangeSearchKernel} for values of type byte.
 */
enum RangeSearchKernelByte implements RangeSearchKernel {

    LT_GT {
        @Override
        void populateAllRangeForEmptyRight(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableIntChunk<? extends Values> o) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, o);
        }

        @Override
        void populateInvalidRanges(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableBooleanChunk<? super Values> v,
                @NotNull final WritableIntChunk<? extends Values> o) {
            populateInvalidRangesDisallowEqual(lsv, lev, v, o);
        }

        @Override
        void findStarts(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findStartsLessThan(lv, lp, rv, rso, rl, o);
        }

        @Override
        void findEnds(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findEndsGreaterThan(lv, lp, rv, rso, rl, o);
        }
    },

    LEQ_GT {
        @Override
        void populateAllRangeForEmptyRight(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableIntChunk<? extends Values> o) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, o);
        }

        @Override
        void populateInvalidRanges(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableBooleanChunk<? super Values> v,
                @NotNull final WritableIntChunk<? extends Values> o) {
            populateInvalidRangesDisallowEqual(lsv, lev, v, o);
        }

        @Override
        void findStarts(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findStartsLessThanEqual(lv, lp, rv, rso, rl, o);
        }

        @Override
        void findEnds(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findEndsGreaterThan(lv, lp, rv, rso, rl, o);
        }
    },

    LEQAP_GT {
        @Override
        void populateAllRangeForEmptyRight(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableIntChunk<? extends Values> o) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, o);
        }

        @Override
        void populateInvalidRanges(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableBooleanChunk<? super Values> v,
                @NotNull final WritableIntChunk<? extends Values> o) {
            populateInvalidRangesDisallowEqual(lsv, lev, v, o);
        }

        @Override
        void findStarts(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findStartsLessThanEqualAllowPreceding(lv, lp, rv, rso, rl, o);
        }

        @Override
        void findEnds(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findEndsGreaterThan(lv, lp, rv, rso, rl, o);
        }
    },

    LT_GEQ {
        @Override
        void populateAllRangeForEmptyRight(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableIntChunk<? extends Values> o) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, o);
        }

        @Override
        void populateInvalidRanges(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableBooleanChunk<? super Values> v,
                @NotNull final WritableIntChunk<? extends Values> o) {
            populateInvalidRangesDisallowEqual(lsv, lev, v, o);
        }

        @Override
        void findStarts(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findStartsLessThan(lv, lp, rv, rso, rl, o);
        }

        @Override
        void findEnds(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findEndsGreaterThanEqual(lv, lp, rv, rso, rl, o);
        }
    },

    LEQ_GEQ {
        @Override
        void populateAllRangeForEmptyRight(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableIntChunk<? extends Values> o) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, o);
        }

        @Override
        void populateInvalidRanges(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableBooleanChunk<? super Values> v,
                @NotNull final WritableIntChunk<? extends Values> o) {
            populateInvalidRangesAllowEqual(lsv, lev, v, o);
        }

        @Override
        void findStarts(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findStartsLessThanEqual(lv, lp, rv, rso, rl, o);
        }

        @Override
        void findEnds(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findEndsGreaterThanEqual(lv, lp, rv, rso, rl, o);
        }
    },

    LEQAP_GEQ {
        @Override
        void populateAllRangeForEmptyRight(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableIntChunk<? extends Values> o) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, o);
        }

        @Override
        void populateInvalidRanges(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableBooleanChunk<? super Values> v,
                @NotNull final WritableIntChunk<? extends Values> o) {
            populateInvalidRangesAllowEqual(lsv, lev, v, o);
        }

        @Override
        void findStarts(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findStartsLessThanEqualAllowPreceding(lv, lp, rv, rso, rl, o);
        }

        @Override
        void findEnds(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findEndsGreaterThanEqual(lv, lp, rv, rso, rl, o);
        }
    },

    LT_GEQAF {
        @Override
        void populateAllRangeForEmptyRight(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableIntChunk<? extends Values> o) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, o);
        }

        @Override
        void populateInvalidRanges(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableBooleanChunk<? super Values> v,
                @NotNull final WritableIntChunk<? extends Values> o) {
            populateInvalidRangesDisallowEqual(lsv, lev, v, o);
        }

        @Override
        void findStarts(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findStartsLessThan(lv, lp, rv, rso, rl, o);
        }

        @Override
        void findEnds(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findEndsGreaterThanEqualAllowFollowing(lv, lp, rv, rso, rl, o);
        }
    },

    LEQ_GEQAF {
        @Override
        void populateAllRangeForEmptyRight(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableIntChunk<? extends Values> o) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, o);
        }

        @Override
        void populateInvalidRanges(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableBooleanChunk<? super Values> v,
                @NotNull final WritableIntChunk<? extends Values> o) {
            populateInvalidRangesAllowEqual(lsv, lev, v, o);
        }

        @Override
        void findStarts(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findStartsLessThanEqual(lv, lp, rv, rso, rl, o);
        }

        @Override
        void findEnds(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findEndsGreaterThanEqualAllowFollowing(lv, lp, rv, rso, rl, o);
        }
    },

    LEQAP_GEQAF {
        @Override
        void populateAllRangeForEmptyRight(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableIntChunk<? extends Values> o) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, o);
        }

        @Override
        void populateInvalidRanges(@NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev, @NotNull final WritableBooleanChunk<? super Values> v,
                @NotNull final WritableIntChunk<? extends Values> o) {
            populateInvalidRangesAllowEqual(lsv, lev, v, o);
        }

        @Override
        void findStarts(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findStartsLessThanEqualAllowPreceding(lv, lp, rv, rso, rl, o);
        }

        @Override
        void findEnds(@NotNull final ByteChunk<? extends Values> lv, @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv, @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl, @NotNull final WritableIntChunk<? extends Values> o) {
            findEndsGreaterThanEqualAllowFollowing(lv, lp, rv, rso, rl, o);
        }
    };

    static RangeSearchKernelByte forRules(
            @NotNull final RangeStartRule startRule,
            @NotNull final RangeEndRule endRule) {
        switch (startRule) {
            case LESS_THAN:
                switch (endRule) {
                    case GREATER_THAN:
                        return LT_GT;
                    case GREATER_THAN_OR_EQUAL:
                        return LT_GEQ;
                    case GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING:
                        return LT_GEQAF;
                }
                break;
            case LESS_THAN_OR_EQUAL:
                switch (endRule) {
                    case GREATER_THAN:
                        return LEQ_GT;
                    case GREATER_THAN_OR_EQUAL:
                        return LEQ_GEQ;
                    case GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING:
                        return LEQ_GEQAF;
                }
                break;
            case LESS_THAN_OR_EQUAL_ALLOW_PRECEDING:
                switch (endRule) {
                    case GREATER_THAN:
                        return LEQAP_GT;
                    case GREATER_THAN_OR_EQUAL:
                        return LEQAP_GEQ;
                    case GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING:
                        return LEQAP_GEQAF;
                }
                break;
        }
        throw new UnsupportedOperationException(String.format(
                "Unrecognized range rule pair {%s, %s}", startRule, endRule));
    }

    @Override
    public final void populateAllRangeForEmptyRight(
            @NotNull final Chunk<? extends Values> leftStartValues,
            @NotNull final Chunk<? extends Values> leftEndValues,
            @NotNull final WritableIntChunk<? extends Values> output) {
        populateAllRangeForEmptyRight(leftStartValues.asByteChunk(), leftEndValues.asByteChunk(), output);
    }

    abstract void populateAllRangeForEmptyRight(
            @NotNull final ByteChunk<? extends Values> leftStartValues,
            @NotNull final ByteChunk<? extends Values> leftEndValues,
            @NotNull final WritableIntChunk<? extends Values> output);

    @Override
    public final void populateInvalidRanges(
            @NotNull final Chunk<? extends Values> leftStartValues,
            @NotNull final Chunk<? extends Values> leftEndValues,
            @NotNull final WritableBooleanChunk<? super Values> validity,
            @NotNull final WritableIntChunk<? extends Values> output) {
        populateInvalidRanges(leftStartValues.asByteChunk(), leftEndValues.asByteChunk(), validity, output);
    }

    abstract void populateInvalidRanges(
            @NotNull final ByteChunk<? extends Values> leftStartValues,
            @NotNull final ByteChunk<? extends Values> leftEndValues,
            @NotNull final WritableBooleanChunk<? super Values> validity,
            @NotNull final WritableIntChunk<? extends Values> output);

    @Override
    public final void findStarts(
            @NotNull final Chunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final Chunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        findStarts(leftValues.asByteChunk(), leftPositions,
                rightValues.asByteChunk(), rightStartOffsets, rightLengths,
                output);
    }

    abstract void findStarts(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output);

    @Override
    public final void findEnds(
            @NotNull final Chunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final Chunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        findEnds(leftValues.asByteChunk(), leftPositions,
                rightValues.asByteChunk(), rightStartOffsets, rightLengths,
                output);
    }

    abstract void findEnds(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output);

    private static void populateInvalidRangesAllowEqual(
            @NotNull ByteChunk<? extends Values> leftStartValues,
            @NotNull ByteChunk<? extends Values> leftEndValues,
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
        validity.setSize(size);
    }

    private static void populateInvalidRangesDisallowEqual(
            @NotNull ByteChunk<? extends Values> leftStartValues,
            @NotNull ByteChunk<? extends Values> leftEndValues,
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
        validity.setSize(size);
    }

    private static void populateAllRangesForEmptyRightAllowEqual(
            @NotNull ByteChunk<? extends Values> leftStartValues,
            @NotNull ByteChunk<? extends Values> leftEndValues,
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

    private static void populateAllRangesForEmptyRightDisallowEqual(
            @NotNull ByteChunk<? extends Values> leftStartValues,
            @NotNull ByteChunk<? extends Values> leftEndValues,
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

    private static boolean isValidPairAllowEqual(final byte start, final byte end) {
        return !isNaN(start) && !isNaN(end) && (isNull(start) || isNull(end) || leq(start, end));
    }

    private static boolean isValidPairDisallowEqual(final byte start, final byte end) {
        return !isNaN(start) && !isNaN(end) && (isNull(start) || isNull(end) || lt(start, end));
    }

    private static void findStartsLessThan(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftStartValues(leftValues, leftPositions, output);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point for not found, or bump by one for found to avoid equal values
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult + 1;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final byte rightValue = rightValues.get(rightIndex);
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we reach rightValue.
            while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeStarts(leftPositions, rightStartOffsets, rightLengths, output, leftIndex);
    }

    private static void findStartsLessThanEqual(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftStartValues(leftValues, leftPositions, output);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point in all cases
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final byte rightValue = rightValues.get(rightIndex);
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we pass rightValue.
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeStarts(leftPositions, rightStartOffsets, rightLengths, output, leftIndex);
    }

    private static void findStartsLessThanEqualAllowPreceding(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftStartValues(leftValues, leftPositions, output);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point in all cases
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final byte rightValue = rightValues.get(rightIndex);
            // Proceed linearly until we have a reason to binary search again
            if (searchResult < 0 && rightIndex != 0) {
                // If we had an inexact match that isn't at the beginning of the right values, look behind
                final int precedingRightPosition =
                        rightStartOffsets.get(rightIndex - 1) + rightLengths.get(rightIndex - 1) - 1;
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), precedingRightPosition);
                // We can re-use precedingRightPosition until we reach rightValue
                while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                    output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)),
                            precedingRightPosition);
                }
            } else {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            }
            // We can re-use rightPosition until we pass rightValue
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeStarts(leftPositions, rightStartOffsets, rightLengths, output, leftIndex);
    }

    /**
     * Process an initial sequence of null left start values, which cause the responsive range to start from right
     * position 0.
     *
     * @param leftValues The left values, sorted according to Deephaven sorting order (nulls first)
     * @param leftPositions The left positions, parallel to {@code leftValues}, used to determine {@code output} index
     * @param output The output chunk
     * @return The number of left indices consumed
     */
    private static int consumeNullLeftStartValues(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final WritableIntChunk<? extends Values> output) {
        final int leftSize = leftValues.size();
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
     * @param leftIndex The left index to start from
     */
    private static void consumeEmptyRangeStarts(
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output,
            int leftIndex) {
        final int leftSize = leftPositions.size();
        final int rightSize = rightStartOffsets.size();
        if (leftIndex == leftSize) {
            return;
        }
        final int rightLastPositionExclusive = rightStartOffsets.get(rightSize - 1) + rightLengths.get(rightSize - 1);
        while (leftIndex < leftSize) {
            output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightLastPositionExclusive);
        }
    }

    private static void findEndsGreaterThan(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftEndValues(leftValues, leftPositions, rightStartOffsets, rightLengths, output);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range ends for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point for not found, or decrement by one for found to avoid equal values
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult - 1;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex) + rightLengths.get(rightIndex) + 1;
            final byte rightValue = rightValues.get(rightIndex);
            output.set(leftPositionToOutputEndPosition(leftPositions.get(leftIndex++)), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we reach rightValue.
            while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                output.set(leftPositionToOutputEndPosition(leftPositions.get(leftIndex++)), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeEnds(leftPositions, rightStartOffsets, rightLengths, output, leftIndex);
    }

    private static void findEndsGreaterThanEqual(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftEndValues(leftValues, leftPositions, rightStartOffsets, rightLengths, output);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range ends for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point in all cases
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex) + rightLengths.get(rightIndex) + 1;
            final byte rightValue = rightValues.get(rightIndex);
            output.set(leftPositionToOutputEndPosition(leftPositions.get(leftIndex++)), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we pass rightValue.
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                output.set(leftPositionToOutputEndPosition(leftPositions.get(leftIndex++)), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeEnds(leftPositions, rightStartOffsets, rightLengths, output, leftIndex);
    }

    private static void findEndsGreaterThanEqualAllowFollowing(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftEndValues(leftValues, leftPositions, rightStartOffsets, rightLengths, output);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range ends for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point in all cases
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            final int rightPosition = rightStartOffsets.get(rightIndex) + rightLengths.get(rightIndex) + 1;
            final byte rightValue = rightValues.get(rightIndex);
            // Proceed linearly until we have a reason to binary search again
            if (searchResult < 0 && rightIndex != rightSize - 1) {
                // If we had an inexact match that isn't at the end of the right values, look ahead
                final int followingRightPosition = rightStartOffsets.get(rightIndex + 1);
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), followingRightPosition);
                // We can re-use followingRightPosition until we reach rightValue
                while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                    output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)),
                            followingRightPosition);
                }
            } else {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            }
            // We can re-use rightPosition until we pass rightValue
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                output.set(leftPositionToOutputStartPosition(leftPositions.get(leftIndex++)), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeEnds(leftPositions, rightStartOffsets, rightLengths, output, leftIndex);
    }

    /**
     * Process an initial sequence of null left end values, which cause the responsive range to end from right position.
     *
     * @param leftValues The left values, sorted according to Deephaven sorting order (nulls first)
     * @param leftPositions The left positions, parallel to {@code leftValues}, used to determine {@code output} index
     * @param rightStartOffsets The right run start offsets
     * @param rightLengths The right run lengths
     * @param output The output chunk
     * @return The number of left indices consumed
     */
    private static int consumeNullLeftEndValues(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output) {
        final int leftSize = leftValues.size();
        final int rightSize = rightStartOffsets.size();
        final int rightLastPositionExclusive = rightStartOffsets.get(rightSize - 1) + rightLengths.get(rightSize - 1);
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            output.set(leftPositionToOutputEndPosition(leftPositions.get(leftIndex++)), rightLastPositionExclusive);
        }
        return leftIndex;
    }

    /**
     * Fill range ends for left end values with no responsive right values.
     *
     * @param leftPositions The left positions, used to determine {@code output} index
     * @param rightStartOffsets The right run start offsets
     * @param rightLengths The right run lengths
     * @param output The output chunk
     * @param leftIndex The left index to start from
     */
    private static void consumeEmptyRangeEnds(
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> output,
            int leftIndex) {
        final int leftSize = leftPositions.size();
        final int rightSize = rightStartOffsets.size();
        if (leftIndex == leftSize) {
            return;
        }
        final int rightLastPositionExclusive = rightStartOffsets.get(rightSize - 1) + rightLengths.get(rightSize - 1);
        while (leftIndex < leftSize) {
            output.set(leftPositionToOutputEndPosition(leftPositions.get(leftIndex++)), rightLastPositionExclusive);
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    // region isNaN
    private static boolean isNaN(@SuppressWarnings("unused") final byte v) {
        return false;
    }
    // endregion isNaN

    // region isNull
    private static boolean isNull(final byte v) {
        // We need not deal with NaN values here
        return v == NULL_BYTE;
    }
    // endregion isNull

    // region lt
    private static boolean lt(final byte a, final byte b) {
        // We need not deal with null or NaN values here
        return a < b;
    }
    // endregion lt

    // region leq
    private static boolean leq(final byte a, final byte b) {
        // We need not deal with null or NaN values here
        return a <= b;
    }
    // endregion leq
}
