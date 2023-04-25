/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFindRunsKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.findruns;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;

public class DoubleFindRunsKernel {
    /**
     * Find runs of two or more identical values in a sorted chunk.  This is used as part of an overall sort, after the
     * timsort (or other sorting) kernel to identify the runs that must be sorted according to secondary keys.
     *
     * @param sortedValues a chunk of sorted values
     * @param offsetsOut an output chunk, with offsets of starting locations that a run occurred
     * @param lengthsOut an output chunk, parallel to offsetsOut, with the lengths of found runs
     *
     * Note, lengthsOut only contain values greater than one.
     */
    static void findRuns(DoubleChunk sortedValues, WritableIntChunk<ChunkPositions> offsetsOut, WritableIntChunk<ChunkLengths> lengthsOut) {
        offsetsOut.setSize(0);
        lengthsOut.setSize(0);

        findRuns(sortedValues, 0, sortedValues.size(), offsetsOut, lengthsOut, false);
    }

    /**
     * Find runs of identical values in a sorted chunk.  If a single value exists, it is included as run of length 1.
     *
     * @param sortedValues a chunk of sorted values
     * @param offsetsOut an output chunk, with offsets of starting locations that a run occurred
     * @param lengthsOut an output chunk, parallel to offsetsOut, with the lengths of found runs
     */
    public static void findRunsSingles(DoubleChunk sortedValues, WritableIntChunk<ChunkPositions> offsetsOut, WritableIntChunk<ChunkLengths> lengthsOut) {
        offsetsOut.setSize(0);
        lengthsOut.setSize(0);

        findRuns(sortedValues, 0, sortedValues.size(), offsetsOut, lengthsOut, true);
    }

    /**
     * Find runs of two or more identical values in a sorted chunk.  This is used as part of an overall sort, after the
     * timsort (or other sorting) kernel to identify the runs that must be sorted according to secondary keys.
     *
     * @param sortedValues a chunk of sorted values
     * @param offsetsIn the offsets within the chunk to check for runs
     * @param lengthsIn the lengths parallel to offsetsIn for run checking
     * @param offsetsOut an output chunk, with offsets of starting locations that a run occurred
     * @param lengthsOut an output chunk, parallel to offsetsOut, with the lengths of found runs
     *
     * Note, that lengthsIn must contain values greater than 1, and lengthsOut additionally only contain values greater than one
     */
    public static void findRuns(DoubleChunk sortedValues, IntChunk<ChunkPositions> offsetsIn, IntChunk<ChunkLengths> lengthsIn, WritableIntChunk<ChunkPositions> offsetsOut, WritableIntChunk<ChunkLengths> lengthsOut) {
        offsetsOut.setSize(0);
        lengthsOut.setSize(0);

        final int numberRuns = offsetsIn.size();
        for (int run = 0; run < numberRuns; ++run) {
            final int offset = offsetsIn.get(run);
            final int length = lengthsIn.get(run);

            findRuns(sortedValues, offset, length, offsetsOut, lengthsOut, false);
        }
    }

    private static void findRuns(DoubleChunk sortedValues, int offset, int length, WritableIntChunk<ChunkPositions> offsetsOut, WritableIntChunk<ChunkLengths> lengthsOut, boolean includeSingles) {
        if (length == 0) {
            return;
        }
        int startRun = offset;
        int cursor = startRun;
        double last = sortedValues.get(cursor++);
        while (cursor < offset + length) {
            final double next = sortedValues.get(cursor);
            if (neq(last, next)) {
                if (includeSingles || cursor != startRun + 1) {
                    offsetsOut.add(startRun);
                    lengthsOut.add(cursor - startRun);
                }
                startRun = cursor;
            }
            cursor++;
            last = next;
        }
        if (includeSingles || cursor != startRun + 1) {
            offsetsOut.add(startRun);
            lengthsOut.add(cursor - startRun);
        }
    }

    private static boolean neq(double last, double next) {
        // region neq
        return Double.doubleToLongBits(next) != Double.doubleToLongBits(last);
        // endregion neq
    }

    private static class DoubleFindRunsKernelContext implements FindRunsKernel {
        @Override
        public void findRuns(Chunk sortedValues, IntChunk<ChunkPositions> offsetsIn, IntChunk<ChunkLengths> lengthsIn, WritableIntChunk<ChunkPositions> offsetsOut, WritableIntChunk<ChunkLengths> lengthsOut) {
            DoubleFindRunsKernel.findRuns(sortedValues.asDoubleChunk(), offsetsIn, lengthsIn, offsetsOut, lengthsOut);
        }

        @Override
        public void findRuns(Chunk sortedValues, WritableIntChunk<ChunkPositions> offsetsOut, WritableIntChunk<ChunkLengths> lengthsOut) {
            DoubleFindRunsKernel.findRuns(sortedValues.asDoubleChunk(), offsetsOut, lengthsOut);
        }

        @Override
        public void findRunsSingles(Chunk sortedValues, WritableIntChunk<ChunkPositions> offsetsOut, WritableIntChunk<ChunkLengths> lengthsOut) {
            DoubleFindRunsKernel.findRunsSingles(sortedValues.asDoubleChunk(), offsetsOut, lengthsOut);
        }
    }

    private final static FindRunsKernel INSTANCE = new DoubleFindRunsKernelContext();

    public static FindRunsKernel createContext() {
        return INSTANCE;
    }
}
