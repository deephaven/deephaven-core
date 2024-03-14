//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit RangeSearchKernelChar and run "./gradlew replicateRangeSearchKernels" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.RangeEndRule;
import io.deephaven.api.RangeStartRule;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * {@link RangeSearchKernel} for values of type byte.
 */
enum RangeSearchKernelByte implements RangeSearchKernel {

    LT_GT {
        @Override
        void processAllRangesForEmptyRight(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void processInvalidRanges(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processInvalidRangesDisallowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void processRangeStarts(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            processRangeStartsLessThan(lv, lp, rv, rso, rse, ospi);
        }

        @Override
        void processRangeEnds(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processRangeEndsGreaterThan(lv, lp, rv, rso, rse, oepe);
        }
    },

    LEQ_GT {
        @Override
        void processAllRangesForEmptyRight(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void processInvalidRanges(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processInvalidRangesDisallowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void processRangeStarts(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            processRangeStartsLessThanEqual(lv, lp, rv, rso, rse, ospi);
        }

        @Override
        void processRangeEnds(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processRangeEndsGreaterThan(lv, lp, rv, rso, rse, oepe);
        }
    },

    LEQAP_GT {
        @Override
        void processAllRangesForEmptyRight(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void processInvalidRanges(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processInvalidRangesDisallowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void processRangeStarts(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            processRangeStartsLessThanEqualAllowPreceding(lv, lp, rv, rso, rse, ospi);
        }

        @Override
        void processRangeEnds(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processRangeEndsGreaterThan(lv, lp, rv, rso, rse, oepe);
        }
    },

    LT_GEQ {
        @Override
        void processAllRangesForEmptyRight(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void processInvalidRanges(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processInvalidRangesDisallowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void processRangeStarts(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            processRangeStartsLessThan(lv, lp, rv, rso, rse, ospi);
        }

        @Override
        void processRangeEnds(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processRangeEndsGreaterThanEqual(lv, lp, rv, rso, rse, oepe);
        }
    },

    LEQ_GEQ {
        @Override
        void processAllRangesForEmptyRight(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void processInvalidRanges(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processInvalidRangesAllowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void processRangeStarts(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            processRangeStartsLessThanEqual(lv, lp, rv, rso, rse, ospi);
        }

        @Override
        void processRangeEnds(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processRangeEndsGreaterThanEqual(lv, lp, rv, rso, rse, oepe);
        }
    },

    LEQAP_GEQ {
        @Override
        void processAllRangesForEmptyRight(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void processInvalidRanges(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processInvalidRangesAllowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void processRangeStarts(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            processRangeStartsLessThanEqualAllowPreceding(lv, lp, rv, rso, rse, ospi);
        }

        @Override
        void processRangeEnds(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processRangeEndsGreaterThanEqual(lv, lp, rv, rso, rse, oepe);
        }
    },

    LT_GEQAF {
        @Override
        void processAllRangesForEmptyRight(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightDisallowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void processInvalidRanges(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processInvalidRangesDisallowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void processRangeStarts(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            processRangeStartsLessThan(lv, lp, rv, rso, rse, ospi);
        }

        @Override
        void processRangeEnds(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processRangeEndsGreaterThanEqualAllowFollowing(lv, lp, rv, rso, rse, oepe);
        }
    },

    LEQ_GEQAF {
        @Override
        void processAllRangesForEmptyRight(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void processInvalidRanges(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processInvalidRangesAllowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void processRangeStarts(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            processRangeStartsLessThanEqual(lv, lp, rv, rso, rse, ospi);
        }

        @Override
        void processRangeEnds(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processRangeEndsGreaterThanEqualAllowFollowing(lv, lp, rv, rso, rse, oepe);
        }
    },

    LEQAP_GEQAF {
        @Override
        void processAllRangesForEmptyRight(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            populateAllRangesForEmptyRightAllowEqual(lsv, lev, ospi, oepe);
        }

        @Override
        void processInvalidRanges(
                @NotNull final ByteChunk<? extends Values> lsv,
                @NotNull final ByteChunk<? extends Values> lev,
                @NotNull final WritableBooleanChunk<? super Values> validity,
                @NotNull final WritableIntChunk<? extends Values> ospi,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processInvalidRangesAllowEqual(lsv, lev, validity, ospi, oepe);
        }

        @Override
        void processRangeStarts(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> ospi) {
            processRangeStartsLessThanEqualAllowPreceding(lv, lp, rv, rso, rse, ospi);
        }

        @Override
        void processRangeEnds(
                @NotNull final ByteChunk<? extends Values> lv,
                @NotNull final IntChunk<ChunkPositions> lp,
                @NotNull final ByteChunk<? extends Values> rv,
                @NotNull final IntChunk<ChunkPositions> rso,
                int rse,
                @NotNull final WritableIntChunk<? extends Values> oepe) {
            processRangeEndsGreaterThanEqualAllowFollowing(lv, lp, rv, rso, rse, oepe);
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
    public final void processAllRangesForEmptyRight(
            @NotNull final Chunk<? extends Values> leftStartValues,
            @NotNull final Chunk<? extends Values> leftEndValues,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        processAllRangesForEmptyRight(
                leftStartValues.asByteChunk(),
                leftEndValues.asByteChunk(),
                outputStartPositionsInclusive,
                outputEndPositionsExclusive);
    }

    abstract void processAllRangesForEmptyRight(
            @NotNull ByteChunk<? extends Values> leftStartValues,
            @NotNull ByteChunk<? extends Values> leftEndValues,
            @NotNull WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull WritableIntChunk<? extends Values> outputEndPositionsExclusive);

    @Override
    public final void processInvalidRanges(
            @NotNull final Chunk<? extends Values> leftStartValues,
            @NotNull final Chunk<? extends Values> leftEndValues,
            @NotNull final WritableBooleanChunk<? super Values> validity,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        processInvalidRanges(
                leftStartValues.asByteChunk(),
                leftEndValues.asByteChunk(),
                validity,
                outputStartPositionsInclusive,
                outputEndPositionsExclusive);
    }

    abstract void processInvalidRanges(
            @NotNull ByteChunk<? extends Values> leftStartValues,
            @NotNull ByteChunk<? extends Values> leftEndValues,
            @NotNull WritableBooleanChunk<? super Values> validity,
            @NotNull WritableIntChunk<? extends Values> outputStartPositionsInclusive,
            @NotNull WritableIntChunk<? extends Values> outputEndPositionsExclusive);

    @Override
    public final void processRangeStarts(
            @NotNull final Chunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final Chunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            final int rightSizeExpanded,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive) {
        processRangeStarts(
                leftValues.asByteChunk(),
                leftPositions,
                rightValues.asByteChunk(),
                rightStartOffsets,
                rightSizeExpanded,
                outputStartPositionsInclusive);
    }

    abstract void processRangeStarts(
            @NotNull ByteChunk<? extends Values> leftValues,
            @NotNull IntChunk<ChunkPositions> leftPositions,
            @NotNull ByteChunk<? extends Values> rightValues,
            @NotNull IntChunk<ChunkPositions> rightStartOffsets,
            int rightSizeExpanded,
            @NotNull WritableIntChunk<? extends Values> outputStartPositionsInclusive);

    @Override
    public final void processRangeEnds(
            @NotNull final Chunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final Chunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            final int rightSizeExpanded,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        processRangeEnds(
                leftValues.asByteChunk(),
                leftPositions,
                rightValues.asByteChunk(),
                rightStartOffsets,
                rightSizeExpanded,
                outputEndPositionsExclusive);
    }

    abstract void processRangeEnds(
            @NotNull ByteChunk<? extends Values> leftValues,
            @NotNull IntChunk<ChunkPositions> leftPositions,
            @NotNull ByteChunk<? extends Values> rightValues,
            @NotNull IntChunk<ChunkPositions> rightStartOffsets,
            int rightSizeExpanded,
            @NotNull WritableIntChunk<? extends Values> outputEndPositionsExclusive);

    private static void populateAllRangesForEmptyRightAllowEqual(
            @NotNull final ByteChunk<? extends Values> leftStartValues,
            @NotNull final ByteChunk<? extends Values> leftEndValues,
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
            @NotNull final ByteChunk<? extends Values> leftStartValues,
            @NotNull final ByteChunk<? extends Values> leftEndValues,
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

    private static void processInvalidRangesAllowEqual(
            @NotNull final ByteChunk<? extends Values> leftStartValues,
            @NotNull final ByteChunk<? extends Values> leftEndValues,
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

    private static void processInvalidRangesDisallowEqual(
            @NotNull final ByteChunk<? extends Values> leftStartValues,
            @NotNull final ByteChunk<? extends Values> leftEndValues,
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

    private static boolean isValidPairAllowEqual(final byte start, final byte end) {
        return !isNaN(start) && !isNaN(end) && (isNull(start) || isNull(end) || leq(start, end));
    }

    private static boolean isValidPairDisallowEqual(final byte start, final byte end) {
        return !isNaN(start) && !isNaN(end) && (isNull(start) || isNull(end) || lt(start, end));
    }

    private static void processRangeStartsLessThan(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            final int rightSizeExpanded,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        // null left start values imply that the responsive right range starts at position 0
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), 0);
        }
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = eq(leftValue, rightValues.get(rightLowIndexInclusive))
                    ? rightLowIndexInclusive
                    : rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // rightIndex should be the index of the first right value whose first position we want to include, so
            // take the insertion point (when not found) or one past the insertion point (when found)
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult + 1;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final byte rightValue = rightValues.get(rightIndex);
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we reach rightValue.
            while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We've processed all left values that care about rightValue, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        // All remaining ranges start after last right (and are thus empty)
        while (leftIndex < leftSize) {
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightSizeExpanded);
        }
    }

    private static void processRangeStartsLessThanEqual(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            final int rightSizeExpanded,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        // null left start values imply that the responsive right range starts at position 0
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), 0);
        }
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = eq(leftValue, rightValues.get(rightLowIndexInclusive))
                    ? rightLowIndexInclusive
                    : rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // rightIndex should be the index of the first right value whose first position we want to include, so
            // take the insertion point whether found or not
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final byte rightValue = rightValues.get(rightIndex);
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we pass rightValue.
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We've processed all left values that care about rightValue, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        // All remaining ranges start after last right (and are thus empty)
        while (leftIndex < leftSize) {
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), rightSizeExpanded);
        }
    }

    private static void processRangeStartsLessThanEqualAllowPreceding(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            final int rightSizeExpanded,
            @NotNull final WritableIntChunk<? extends Values> outputStartPositionsInclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        // null left start values imply that the responsive right range starts at position 0
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), 0);
        }
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range starts for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = eq(leftValue, rightValues.get(rightLowIndexInclusive))
                    ? rightLowIndexInclusive
                    : rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // rightIndex should be the index of the first right value whose first position we want to include, so
            // take the insertion point whether found or not
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final byte rightValue = rightValues.get(rightIndex);
            // Proceed linearly until we have a reason to binary search again
            if (searchResult < 0 && rightIndex != 0) {
                // If we had an inexact match that isn't at the beginning of the right values, look behind
                final int precedingRightPosition = rightPosition - 1;
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
            // We've processed all left values that care about rightValue, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        // All remaining left start values are after last right value. Last right position is in range by "allow
        // preceding" logic.
        final int lastRightPosition = rightSizeExpanded - 1;
        while (leftIndex < leftSize) {
            outputStartPositionsInclusive.set(leftPositions.get(leftIndex++), lastRightPosition);
        }
    }

    private static void processRangeEndsGreaterThan(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            final int rightSizeExpanded,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        // null left end values imply that the responsive right range ends at rightSizeExpanded (exclusive)
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightSizeExpanded);
        }
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range ends for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = eq(leftValue, rightValues.get(rightLowIndexInclusive))
                    ? rightLowIndexInclusive
                    : rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // rightIndex should be the index of the first right value whose first position we want to exclude, so
            // take the insertion point whether found or not
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final byte rightValue = rightValues.get(rightIndex);
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we pass rightValue.
            while (leftIndex < leftSize && leq(leftValue = leftValues.get(leftIndex), rightValue)) {
                outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We've processed all left values that care about rightValue, so begin searching after rightIndex
            rightLowIndexInclusive = rightIndex + 1;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        // All remaining ranges end at last right
        while (leftIndex < leftSize) {
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightSizeExpanded);
        }
    }

    private static void processRangeEndsGreaterThanEqual(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            final int rightSizeExpanded,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        // null left end values imply that the responsive right range ends at rightSizeExpanded (exclusive)
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightSizeExpanded);
        }
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range ends for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = eq(leftValue, rightValues.get(rightLowIndexInclusive))
                    ? rightLowIndexInclusive
                    : rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // rightIndex should be the index of the first right value whose first position we want to exclude, so
            // take the insertion point (when not found) or one past the insertion point (when found)
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult + 1;
            if (rightIndex == rightSize) {
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final byte rightValue = rightValues.get(rightIndex);
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
            // Proceed linearly until we have a reason to binary search again. We can re-use rightPosition until
            // we reach rightValue.
            while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
            }
            // We've processed all left values that can exclude rightValue, so begin searching at rightIndex
            rightLowIndexInclusive = rightIndex;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        // All remaining ranges end at last right
        while (leftIndex < leftSize) {
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightSizeExpanded);
        }
    }

    private static void processRangeEndsGreaterThanEqualAllowFollowing(
            @NotNull final ByteChunk<? extends Values> leftValues,
            @NotNull final IntChunk<ChunkPositions> leftPositions,
            @NotNull final ByteChunk<? extends Values> rightValues,
            @NotNull final IntChunk<ChunkPositions> rightStartOffsets,
            final int rightSizeExpanded,
            @NotNull final WritableIntChunk<? extends Values> outputEndPositionsExclusive) {
        // Note that invalid and undefined ranges have already been eliminated
        final int leftSize = leftValues.size();
        final int rightSize = rightValues.size();

        // Empty rights are handled via a different method
        Assert.gtZero(rightSize, "rightSize");

        // null left end values imply that the responsive right range ends at rightSizeExpanded (exclusive)
        int leftIndex = 0;
        while (leftIndex < leftSize && isNull(leftValues.get(leftIndex))) {
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightSizeExpanded);
        }
        if (leftIndex == leftSize) {
            return;
        }

        // Find the range ends for non-null left values
        int rightLowIndexInclusive = 0;
        byte leftValue = leftValues.get(leftIndex);
        do {
            final int searchResult = eq(leftValue, rightValues.get(rightLowIndexInclusive))
                    ? rightLowIndexInclusive
                    : rightValues.binarySearch(rightLowIndexInclusive, rightSize, leftValue);
            // rightIndex should be the index of the first right value whose first position we want to exclude, so
            // take the insertion point (when not found) or one past the insertion point (when found)
            final int rightIndex = searchResult < 0 ? ~searchResult : searchResult + 1;
            if (rightIndex == rightSize) {
                // This left value and all remaining left values are at or after last right value. All right positions
                // are within this bound.
                break;
            }
            final int rightPosition = rightStartOffsets.get(rightIndex);
            final byte rightValue = rightValues.get(rightIndex);
            // Proceed linearly until we have a reason to binary search again
            if (searchResult < 0) {
                // If we had an inexact match that isn't at the end of the right values, look ahead
                final int followingRightPosition = rightPosition + 1;
                if (followingRightPosition == rightSizeExpanded) {
                    // We're going to fill the remainder with rightSizeExpanded, and we don't need any value comparisons
                    // to figure that out
                    break;
                }
                outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), followingRightPosition);
                // We can re-use followingRightPosition until we reach rightValue
                while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                    outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), followingRightPosition);
                }
            } else {
                outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
                // We can re-use rightPosition until we reach rightValue
                while (leftIndex < leftSize && lt(leftValue = leftValues.get(leftIndex), rightValue)) {
                    outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightPosition);
                }
            }
            // We've processed all left values that can exclude rightValue, so begin searching at rightIndex
            rightLowIndexInclusive = rightIndex;
        } while (leftIndex < leftSize && rightLowIndexInclusive < rightSize);

        // All remaining ranges end at last right
        while (leftIndex < leftSize) {
            outputEndPositionsExclusive.set(leftPositions.get(leftIndex++), rightSizeExpanded);
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

    // region eq
    private static boolean eq(final byte a, final byte b) {
        // We need not deal with null or NaN values here
        return a == b;
    }
    // endregion eq

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
