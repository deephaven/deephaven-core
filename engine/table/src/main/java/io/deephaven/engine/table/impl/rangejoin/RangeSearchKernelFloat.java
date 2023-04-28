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

import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * {@link RangeSearchKernel} for values of type float.
 */
enum RangeSearchKernelFloat implements RangeSearchKernel {

    LT_GT {
        @Override
        void populateAllRangeForEmptyRight(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void populateInvalidRanges(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateInvalidRangesDisallowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void findStarts(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            findStartsLessThan(lv, lp, rv, rso, rl, ospi);
        }

        @Override
        void findEnds(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            findEndsGreaterThan(lv, lp, rv, rso, rl, oepe);
        }
    },

    LEQ_GT {
        @Override
        void populateAllRangeForEmptyRight(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void populateInvalidRanges(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateInvalidRangesDisallowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void findStarts(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            findStartsLessThanEqual(lv, lp, rv, rso, rl, ospi);
        }

        @Override
        void findEnds(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            findEndsGreaterThan(lv, lp, rv, rso, rl, oepe);
        }
    },

    LEQAP_GT {
        @Override
        void populateAllRangeForEmptyRight(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void populateInvalidRanges(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateInvalidRangesDisallowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void findStarts(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            findStartsLessThanEqualAllowPreceding(lv, lp, rv, rso, rl, ospi);
        }

        @Override
        void findEnds(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            findEndsGreaterThan(lv, lp, rv, rso, rl, oepe);
        }
    },

    LT_GEQ {
        @Override
        void populateAllRangeForEmptyRight(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void populateInvalidRanges(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateInvalidRangesDisallowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void findStarts(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            findStartsLessThan(lv, lp, rv, rso, rl, ospi);
        }

        @Override
        void findEnds(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            findEndsGreaterThanEqual(lv, lp, rv, rso, rl, oepe);
        }
    },

    LEQ_GEQ {
        @Override
        void populateAllRangeForEmptyRight(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void populateInvalidRanges(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateInvalidRangesAllowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void findStarts(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            findStartsLessThanEqual(lv, lp, rv, rso, rl, ospi);
        }

        @Override
        void findEnds(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            findEndsGreaterThanEqual(lv, lp, rv, rso, rl, oepe);
        }
    },

    LEQAP_GEQ {
        @Override
        void populateAllRangeForEmptyRight(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void populateInvalidRanges(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateInvalidRangesAllowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void findStarts(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            findStartsLessThanEqualAllowPreceding(lv, lp, rv, rso, rl, ospi);
        }

        @Override
        void findEnds(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            findEndsGreaterThanEqual(lv, lp, rv, rso, rl, oepe);
        }
    },

    LT_GEQAF {
        @Override
        void populateAllRangeForEmptyRight(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void populateInvalidRanges(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateInvalidRangesDisallowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void findStarts(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            findStartsLessThan(lv, lp, rv, rso, rl, ospi);
        }

        @Override
        void findEnds(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            findEndsGreaterThanEqualAllowFollowing(lv, lp, rv, rso, rl, oepe);
        }
    },

    LEQ_GEQAF {
        @Override
        void populateAllRangeForEmptyRight(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void populateInvalidRanges(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateInvalidRangesAllowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void findStarts(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            findStartsLessThanEqual(lv, lp, rv, rso, rl, ospi);
        }

        @Override
        void findEnds(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            findEndsGreaterThanEqualAllowFollowing(lv, lp, rv, rso, rl, oepe);
        }
    },

    LEQAP_GEQAF {
        @Override
        void populateAllRangeForEmptyRight(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void populateInvalidRanges(
                @NotNull final FloatChunk<? extends Values> lsv,
                @NotNull final FloatChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateInvalidRangesAllowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void findStarts(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            findStartsLessThanEqualAllowPreceding(lv, lp, rv, rso, rl, ospi);
        }

        @Override
        void findEnds(
                @NotNull final FloatChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final FloatChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                @NotNull final IntChunk<ChunkLengths> rl,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            findEndsGreaterThanEqualAllowFollowing(lv, lp, rv, rso, rl, oepe);
        }
    };

    static RangeSearchKernelFloat forRules(
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
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        populateAllRangeForEmptyRight(
                leftStartValues.asFloatChunk(),
                leftEndValues.asFloatChunk(),
                outputStartPositionsInclusive,
                outputEndPositionsExclusive);
    }

    abstract void populateAllRangeForEmptyRight(
            @NotNull FloatChunk<? extends Values> leftStartValues,
            @NotNull FloatChunk<? extends Values> leftEndValues,
            @NotNull WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull WritableIntChunk<? extends Values> outputEndPositionsExclusive);

    @Override
    public final void populateInvalidRanges(
            @NotNull final Chunk<? extends Values> leftStartValues,
            @NotNull final Chunk<? extends Values> leftEndValues,
            @NotNull final WritableBooleanChunk<? super Values> validity,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        populateInvalidRanges(
                leftStartValues.asFloatChunk(),
                leftEndValues.asFloatChunk(),
                validity,
                outputStartPositionsInclusive,
                outputEndPositionsExclusive);
    }

    abstract void populateInvalidRanges(
            @NotNull FloatChunk<? extends Values> leftStartValues,
            @NotNull FloatChunk<? extends Values> leftEndValues,
            @NotNull WritableBooleanChunk<? super Values> validity,
            @NotNull WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull WritableIntChunk<? extends Values> outputEndPositionsExclusive);

    @Override
    public final void findStarts(
            @NotNull final Chunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final Chunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive) {
        findStarts(
                leftValues.asFloatChunk(),
                leftPositions,
                rightValues.asFloatChunk(),
                rightStartOffsets,
                rightLengths,
                outputStartPositionsInclusive);
    }

    abstract void findStarts(
            @NotNull FloatChunk<? extends Values> leftValues,
            @NotNull IntChunk<ChunkPositions> leftPositions,
            @NotNull FloatChunk<? extends Values> rightValues,
            @NotNull IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull IntChunk<ChunkLengths> rightLengths,
            @NotNull WritableIntChunk<? extends Values> outputStartPositionsInclusive);

    @Override
    public final void findEnds(
            @NotNull final Chunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final Chunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        findEnds(
                leftValues.asFloatChunk(),
                leftPositions,
                rightValues.asFloatChunk(),
                rightStartOffsets,
                rightLengths,
                outputEndPositionsExclusive);
    }

    abstract void findEnds(
            @NotNull FloatChunk<? extends Values> leftValues,
            @NotNull IntChunk<ChunkPositions> leftPositions,
            @NotNull FloatChunk<? extends Values> rightValues,
            @NotNull IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull IntChunk<ChunkLengths> rightLengths,
            @NotNull WritableIntChunk<? extends Values> outputEndPositionsExclusive);

    private static void populateAllRangesForEmptyRightAllowEqual(
            @NotNull final FloatChunk<? extends Values> leftStartValues,
            @NotNull final FloatChunk<? extends Values> leftEndValues,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        final int size = leftStartValues.size();
        Assert.eq(size, "leftStartValues.size()", leftEndValues.size(), "leftEndValues.size()");

        for (int li = 0; li < size; ++li) {
            if (isValidPairAllowEqual(leftStartValues.get(li), leftEndValues.get(li))) {
                outputStartPositionsInclusive.set(li, 0);
                outputEndPositionsExclusive.set((li), 0);
            } else {
                outputStartPositionsInclusive.set(li, NULL_INT);
                outputEndPositionsExclusive.set(li, NULL_INT);
            }
        }
        outputStartPositionsInclusive.setSize(size);
        outputEndPositionsExclusive.setSize(size);
    }

    private static void populateAllRangesForEmptyRightDisallowEqual(
            @NotNull final FloatChunk<? extends Values> leftStartValues,
            @NotNull final FloatChunk<? extends Values> leftEndValues,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        final int size = leftStartValues.size();
        Assert.eq(size, "leftStartValues.size()", leftEndValues.size(), "leftEndValues.size()");

        for (int li = 0; li < size; ++li) {
            if (isValidPairDisallowEqual(leftStartValues.get(li), leftEndValues.get(li))) {
                outputStartPositionsInclusive.set(li, 0);
                outputEndPositionsExclusive.set(li, 0);
            } else {
                outputStartPositionsInclusive.set(li, NULL_INT);
                outputEndPositionsExclusive.set(li, NULL_INT);
            }
        }
        outputStartPositionsInclusive.setSize(size);
        outputEndPositionsExclusive.setSize(size);
    }

    private static void populateInvalidRangesAllowEqual(
            @NotNull final FloatChunk<? extends Values> leftStartValues,
            @NotNull final FloatChunk<? extends Values> leftEndValues,
            @NotNull final WritableBooleanChunk<? super Values> validity,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        final int size = leftStartValues.size();
        Assert.eq(size, "leftStartValues.size()", leftEndValues.size(), "leftEndValues.size()");

        for (int li = 0; li < size; ++li) {
            if (isValidPairAllowEqual(leftStartValues.get(li), leftEndValues.get(li))) {
                validity.set(li, true);
            } else {
                validity.set(li, false);
                outputStartPositionsInclusive.set(li, NULL_INT);
                outputEndPositionsExclusive.set(li, NULL_INT);
            }
        }
        validity.setSize(size);
        outputStartPositionsInclusive.setSize(size);
        outputEndPositionsExclusive.setSize(size);
    }

    private static void populateInvalidRangesDisallowEqual(
            @NotNull final FloatChunk<? extends Values> leftStartValues,
            @NotNull final FloatChunk<? extends Values> leftEndValues,
            @NotNull final WritableBooleanChunk<? super Values> validity,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        final int size = leftStartValues.size();
        Assert.eq(size, "leftStartValues.size()", leftEndValues.size(), "leftEndValues.size()");

        for (int li = 0; li < size; ++li) {
            if (isValidPairDisallowEqual(leftStartValues.get(li), leftEndValues.get(li))) {
                validity.set(li, true);
            } else {
                validity.set(li, false);
                outputStartPositionsInclusive.set(li, NULL_INT);
                outputEndPositionsExclusive.set(li, NULL_INT);
            }
        }
        validity.setSize(size);
        outputStartPositionsInclusive.setSize(size);
        outputEndPositionsExclusive.setSize(size);
    }

    private static boolean isValidPairAllowEqual(final float start, final float end) {
        return !isNaN(start) && !isNaN(end) && (isNull(start) || isNull(end) || leq(start, end));
    }

    private static boolean isValidPairDisallowEqual(final float start, final float end) {
        return !isNaN(start) && !isNaN(end) && (isNull(start) || isNull(end) || lt(start, end));
    }

    private static void findStartsLessThan(
            @NotNull final FloatChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final FloatChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftStartValues(leftValues, leftPositions, outputStartPositionsInclusive);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        float leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point for not found, or bump by one for found to avoid equal values
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult + 1;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final float rightValue = rightValues.get(rightIndex);
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we reach rightValue.
            while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeStarts(
                leftPositions, rightStartOffsets, rightLengths, outputStartPositionsInclusive, leftIndex);
    }

    private static void findStartsLessThanEqual(
            @NotNull final FloatChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final FloatChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftStartValues(leftValues, leftPositions, outputStartPositionsInclusive);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        float leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point in all cases
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final float rightValue = rightValues.get(rightIndex);
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we pass rightValue.
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeStarts(
                leftPositions, rightStartOffsets, rightLengths, outputStartPositionsInclusive, leftIndex);
    }

    private static void findStartsLessThanEqualAllowPreceding(
            @NotNull final FloatChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final FloatChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftStartValues(leftValues, leftPositions, outputStartPositionsInclusive);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        float leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point in all cases
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final float rightValue = rightValues.get(rightIndex);
            // Proceed linearly until we have a reason to binary search again
            if (searchResult < 0 && rightIndex != 0) {
                // If we had an inexact match that isn't at the beginning of the right values, look behind
                final int precedingRightPosition =
                        rightStartOffsets.get(rightIndex - 1) + rightLengths.get(rightIndex - 1) - 1;
                outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), precedingRightPosition);
                // We can re-use precedingRightPosition until we reach rightValue
                while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                    outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), precedingRightPosition);
                }
            } else {
                outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We can re-use rightPosition until we pass rightValue
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeStarts(
                leftPositions, rightStartOffsets, rightLengths, outputStartPositionsInclusive, leftIndex);
    }

    /**
     * Process an initial sequence of null left start values, which cause the responsive range to start from right
     * position 0.
     *
     * @param leftValues The left values, sorted according to Deephaven sorting order (nulls first)
     * @param leftPositions The left positions, parallel to {@code leftValues}, used to determine {@code output} index
     * @param outputStartPositionsInclusive The output chunk
     * @return The number of left indices consumed
     */
    private static int consumeNullLeftStartValues(
            @NotNull final FloatChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive) {
        final int leftSize = leftValues.size();
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), 0);
        }
        return leftIndex;
    }

    /**
     * Fill range starts for left start values with no responsive right values.
     *
     * @param leftPositions The left positions, used to determine {@code output} index
     * @param rightStartOffsets The right run start offsets
     * @param rightLengths The right run lengths
     * @param outputStartPositionsInclusive The output chunk
     * @param leftIndex The left index to start from
     */
    private static void consumeEmptyRangeStarts(
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            int leftIndex) {
        final int leftSize = leftPositions.size();
        final int rightSize = rightStartOffsets.size();
        if (leftIndex == leftSize) {
            return;
        }
        final int rightLastPositionExclusive = rightStartOffsets.get(rightSize - 1) + rightLengths.get(rightSize - 1);
        while (leftIndex < leftSize) {
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightLastPositionExclusive);
        }
    }

    private static void findEndsGreaterThan(
            @NotNull final FloatChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final FloatChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftEndValues(
                leftValues, leftPositions, rightStartOffsets, rightLengths, outputEndPositionsExclusive);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range ends for non-null left values
        int rightLowIndexInclusive = 0;
        float leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point for not found, or decrement by one for found to avoid equal values
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult - 1;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex) + rightLengths.get(rightIndex) + 1;
            final float rightValue = rightValues.get(rightIndex);
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we reach rightValue.
            while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeEnds(leftPositions, rightStartOffsets, rightLengths, outputEndPositionsExclusive, leftIndex);
    }

    private static void findEndsGreaterThanEqual(
            @NotNull final FloatChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final FloatChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftEndValues(
                leftValues, leftPositions, rightStartOffsets, rightLengths, outputEndPositionsExclusive);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range ends for non-null left values
        int rightLowIndexInclusive = 0;
        float leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point in all cases
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex) + rightLengths.get(rightIndex) + 1;
            final float rightValue = rightValues.get(rightIndex);
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we pass rightValue.
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeEnds(leftPositions, rightStartOffsets, rightLengths, outputEndPositionsExclusive, leftIndex);
    }

    private static void findEndsGreaterThanEqualAllowFollowing(
            @NotNull final FloatChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final FloatChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        int leftIndex = consumeNullLeftEndValues(
                leftValues, leftPositions, rightStartOffsets, rightLengths, outputEndPositionsExclusive);
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range ends for non-null left values
        int rightLowIndexInclusive = 0;
        float leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // Take insertion point in all cases
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            final int rightPosition = rightStartOffsets.get(rightIndex) + rightLengths.get(rightIndex) + 1;
            final float rightValue = rightValues.get(rightIndex);
            // Proceed linearly until we have a reason to binary search again
            if (searchResult < 0 && rightIndex != rightSize - 1) {
                // If we had an inexact match that isn't at the end of the right values, look ahead
                final int followingRightPosition = rightStartOffsets.get(rightIndex + 1);
                outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), followingRightPosition);
                // We can re-use followingRightPosition until we reach rightValue
                while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                    outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), followingRightPosition);
                }
            } else {
                outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We can re-use rightPosition until we pass rightValue
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We've consumed all left values that can match rightIndex, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        consumeEmptyRangeEnds(leftPositions, rightStartOffsets, rightLengths, outputEndPositionsExclusive, leftIndex);
    }

    /**
     * Process an initial sequence of null left end values, which cause the responsive range to end from right position.
     *
     * @param leftValues The left values, sorted according to Deephaven sorting order (nulls first)
     * @param leftPositions The left positions, parallel to {@code leftValues}, used to determine {@code output} index
     * @param rightStartOffsets The right run start offsets
     * @param rightLengths The right run lengths
     * @param outputEndPositionsExclusive The output chunk
     * @return The number of left indices consumed
     */
    private static int consumeNullLeftEndValues(
            @NotNull final FloatChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        final int leftSize = leftValues.size();
        final int rightSize = rightStartOffsets.size();
        final int rightLastPositionExclusive = rightStartOffsets.get(rightSize - 1) + rightLengths.get(rightSize - 1);
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightLastPositionExclusive);
        }
        return leftIndex;
    }

    /**
     * Fill range ends for left end values with no responsive right values.
     *
     * @param leftPositions The left positions, used to determine {@code output} index
     * @param rightStartOffsets The right run start offsets
     * @param rightLengths The right run lengths
     * @param outputEndPositionsExclusive The output chunk
     * @param leftIndex The left index to start from
     */
    private static void consumeEmptyRangeEnds(
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            @NotNull final IntChunk<ChunkLengths> rightLengths,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive,
            int leftIndex) {
        final int leftSize = leftPositions.size();
        final int rightSize = rightStartOffsets.size();
        if (leftIndex == leftSize) {
            return;
        }
        final int rightLastPositionExclusive = rightStartOffsets.get(rightSize - 1) + rightLengths.get(rightSize - 1);
        while (leftIndex < leftSize) {
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightLastPositionExclusive);
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    // region isNaN
    private static boolean isNaN(final float v) {
        return Float.isNaN(v);
    }
    // endregion isNaN

    // region isNull
    private static boolean isNull(final float v) {
        // We need not deal with NaN values here
        return v == NULL_FLOAT;
    }
    // endregion isNull

    // region lt
    private static boolean lt(final float a, final float b) {
        // We need not deal with null or NaN values here
        return a < b;
    }
    // endregion lt

    // region leq
    private static boolean leq(final float a, final float b) {
        // We need not deal with null or NaN values here
        return a <= b;
    }
    // endregion leq
}
