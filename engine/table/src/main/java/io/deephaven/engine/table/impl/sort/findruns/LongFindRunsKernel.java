//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharFindRunsKernel and run "./gradlew replicateSortKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort.findruns;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.compare.LongComparisons.eq;
import static io.deephaven.util.compare.LongComparisons.leq;

public class LongFindRunsKernel {
    /**
     * Find runs of two or more identical values in a sorted chunk. This is used as part of an overall sort, after the
     * timsort (or other sorting) kernel to identify the runs that must be sorted according to secondary keys.
     *
     * @param sortedValues a chunk of sorted values
     * @param offsetsOut an output chunk, with offsets of starting locations that a run occurred
     * @param lengthsOut an output chunk, parallel to offsetsOut, with the lengths of found runs
     *
     *        Note, lengthsOut only contain values greater than one.
     */
    static void findRuns(
            @NotNull final LongChunk<? extends Any> sortedValues,
            @NotNull final WritableIntChunk<ChunkPositions> offsetsOut,
            @NotNull final WritableIntChunk<ChunkLengths> lengthsOut) {
        offsetsOut.setSize(0);
        lengthsOut.setSize(0);

        findRuns(sortedValues, 0, sortedValues.size(), offsetsOut, lengthsOut, false);
    }

    /**
     * Find runs of identical values in a sorted chunk. If a single value exists, it is included as run of length 1.
     *
     * @param sortedValues a chunk of sorted values
     * @param offsetsOut an output chunk, with offsets of starting locations that a run occurred
     * @param lengthsOut an output chunk, parallel to offsetsOut, with the lengths of found runs
     */
    public static void findRunsSingles(
            @NotNull final LongChunk<? extends Any> sortedValues,
            @NotNull final WritableIntChunk<ChunkPositions> offsetsOut,
            @NotNull final WritableIntChunk<ChunkLengths> lengthsOut) {
        offsetsOut.setSize(0);
        lengthsOut.setSize(0);

        findRuns(sortedValues, 0, sortedValues.size(), offsetsOut, lengthsOut, true);
    }

    /**
     * Find runs of two or more identical values in a sorted chunk. This is used as part of an overall sort, after the
     * timsort (or other sorting) kernel to identify the runs that must be sorted according to secondary keys.
     *
     * @param sortedValues a chunk of sorted values
     * @param offsetsIn the offsets within the chunk to check for runs
     * @param lengthsIn the lengths parallel to offsetsIn for run checking
     * @param offsetsOut an output chunk, with offsets of starting locations that a run occurred
     * @param lengthsOut an output chunk, parallel to offsetsOut, with the lengths of found runs
     *
     *        Note, that lengthsIn must contain values greater than 1, and lengthsOut additionally only contain values
     *        greater than one
     */
    public static void findRuns(
            @NotNull final LongChunk<? extends Any> sortedValues,
            @NotNull final IntChunk<ChunkPositions> offsetsIn,
            @NotNull final IntChunk<ChunkLengths> lengthsIn,
            @NotNull final WritableIntChunk<ChunkPositions> offsetsOut,
            @NotNull final WritableIntChunk<ChunkLengths> lengthsOut) {
        offsetsOut.setSize(0);
        lengthsOut.setSize(0);

        final int numberRuns = offsetsIn.size();
        for (int run = 0; run < numberRuns; ++run) {
            final int offset = offsetsIn.get(run);
            final int length = lengthsIn.get(run);

            findRuns(sortedValues, offset, length, offsetsOut, lengthsOut, false);
        }
    }

    private static void findRuns(
            @NotNull final LongChunk<? extends Any> sortedValues,
            final int offset,
            final int length,
            @NotNull final WritableIntChunk<ChunkPositions> offsetsOut,
            @NotNull final WritableIntChunk<ChunkLengths> lengthsOut,
            final boolean includeSingles) {
        if (length == 0) {
            return;
        }
        int startRun = offset;
        int cursor = startRun;
        long last = sortedValues.get(cursor++);
        while (cursor < offset + length) {
            final long next = sortedValues.get(cursor);
            if (!eq(last, next)) {
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

    public static int compactRuns(
            @NotNull final WritableLongChunk<? extends Any> sortedValues,
            @NotNull final IntChunk<ChunkPositions> offsetsIn) {
        final int numRuns = offsetsIn.size();
        if (numRuns == 0) {
            return -1;
        }
        long last = sortedValues.get(offsetsIn.get(0));
        sortedValues.set(0, last);
        for (int ri = 1; ri < numRuns; ++ri) {
            final int nextOffset = offsetsIn.get(ri);
            final long next = sortedValues.get(nextOffset);
            if (leq(next, last)) {
                return ri;
            }
            sortedValues.set(ri, next);
            last = next;
        }
        sortedValues.setSize(numRuns);
        return -1;
    }

    private static class LongFindRunsKernelContext implements FindRunsKernel {

        @Override
        public void findRuns(
                @NotNull final Chunk<? extends Any> sortedValues,
                @NotNull final WritableIntChunk<ChunkPositions> offsetsOut,
                @NotNull final WritableIntChunk<ChunkLengths> lengthsOut) {
            LongFindRunsKernel.findRuns(sortedValues.asLongChunk(), offsetsOut, lengthsOut);
        }

        @Override
        public void findRunsSingles(
                @NotNull final Chunk<? extends Any> sortedValues,
                @NotNull final WritableIntChunk<ChunkPositions> offsetsOut,
                @NotNull final WritableIntChunk<ChunkLengths> lengthsOut) {
            LongFindRunsKernel.findRunsSingles(sortedValues.asLongChunk(), offsetsOut, lengthsOut);
        }

        @Override
        public void findRuns(
                @NotNull final Chunk<? extends Any> sortedValues,
                @NotNull final IntChunk<ChunkPositions> offsetsIn,
                @NotNull final IntChunk<ChunkLengths> lengthsIn,
                @NotNull final WritableIntChunk<ChunkPositions> offsetsOut,
                @NotNull final WritableIntChunk<ChunkLengths> lengthsOut) {
            LongFindRunsKernel.findRuns(sortedValues.asLongChunk(), offsetsIn, lengthsIn, offsetsOut, lengthsOut);
        }

        @Override
        public int compactRuns(
                @NotNull final WritableChunk<? extends Any> sortedValues,
                @NotNull final IntChunk<ChunkPositions> offsetsIn) {
            return LongFindRunsKernel.compactRuns(sortedValues.asWritableLongChunk(), offsetsIn);
        }
    }

    private final static FindRunsKernel INSTANCE = new LongFindRunsKernelContext();

    public static FindRunsKernel getInstance() {
        return INSTANCE;
    }
}
