//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.timsort;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;
import io.deephaven.engine.table.impl.sort.timsort.indirect.IndirectTimsortDispatcher;
import io.deephaven.util.SafeCloseable;
import org.junit.Test;

import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Comparison counts of the generated timsort kernels, observed through their comparator variants: the direct
 * {@link ComparatorLongTimsortKernel} and the indirect comparator kernel from {@link IndirectTimsortDispatcher}. Every
 * timsort kernel (direct, indirect, and runtime-compiled multi-column) is generated from the same merge logic, so the
 * counts apply to all of them.
 */
public class TestComparatorTimsortKernels {
    /** Counts the comparisons made over Integer values. */
    private static final class CountingComparator implements Comparator<Object> {
        private long count;

        @Override
        public int compare(final Object lhs, final Object rhs) {
            ++count;
            return Integer.compare((Integer) lhs, (Integer) rhs);
        }
    }

    /** A sort kernel context over Integer values that sorts or merges a chunk and verifies the result. */
    private interface IntegerKernel extends SafeCloseable {
        /** Sorts values in place. */
        void sort(Integer[] values);

        /** Merges the adjacent sorted runs [0, firstRunLength) and [firstRunLength, values.length) in place. */
        void merge(Integer[] values, int firstRunLength);
    }

    /** The direct kernel, which moves the values and a parallel chunk of row keys. */
    private static final class DirectKernel implements IntegerKernel {
        private final WritableObjectChunk<Object, Any> chunk;
        private final WritableLongChunk<Any> payload;
        private final LongSortKernel<Any, Any> kernel;

        private DirectKernel(final int size, final Comparator<Object> comparator) {
            chunk = WritableObjectChunk.makeWritableChunk(size);
            payload = WritableLongChunk.makeWritableChunk(size);
            kernel = ComparatorLongTimsortKernel.createContext(size, comparator);
        }

        private void load(final Integer[] values) {
            chunk.setSize(values.length);
            payload.setSize(values.length);
            for (int ii = 0; ii < values.length; ++ii) {
                chunk.set(ii, values[ii]);
                payload.set(ii, ii);
            }
        }

        private void unload(final Integer[] values) {
            for (int ii = 0; ii < values.length; ++ii) {
                values[ii] = (Integer) chunk.get(ii);
            }
        }

        @Override
        public void sort(final Integer[] values) {
            load(values);
            kernel.sort(payload, chunk);
            unload(values);
        }

        @Override
        public void merge(final Integer[] values, final int firstRunLength) {
            load(values);
            kernel.merge(payload, chunk, 0, firstRunLength, values.length - firstRunLength);
            unload(values);
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(chunk, payload, kernel);
        }
    }

    /** The indirect kernel, which permutes positions into a values chunk that it never moves. */
    private static final class IndirectKernel implements IntegerKernel {
        private final WritableObjectChunk<Object, Any> chunk;
        private final WritableLongChunk<Any> payload;
        private final WritableIntChunk<ChunkPositions> positions;
        private final MultiColumnSortKernel<Any> kernel;

        private IndirectKernel(final int size, final Comparator<Object> comparator) {
            chunk = WritableObjectChunk.makeWritableChunk(size);
            payload = WritableLongChunk.makeWritableChunk(size);
            positions = WritableIntChunk.makeWritableChunk(size);
            kernel = IndirectTimsortDispatcher.makeContext(new ChunkType[] {ChunkType.Object},
                    new SortingOrder[] {SortingOrder.Ascending}, new Comparator[] {comparator}, size);
        }

        private void load(final Integer[] values) {
            chunk.setSize(values.length);
            payload.setSize(values.length);
            positions.setSize(values.length);
            for (int ii = 0; ii < values.length; ++ii) {
                chunk.set(ii, values[ii]);
                payload.set(ii, ii);
                positions.set(ii, ii);
            }
        }

        @Override
        public void sort(final Integer[] values) {
            load(values);
            kernel.sort(payload, new WritableChunk[] {chunk});
            for (int ii = 0; ii < values.length; ++ii) {
                values[ii] = (Integer) chunk.get((int) payload.get(ii));
            }
        }

        @Override
        public void merge(final Integer[] values, final int firstRunLength) {
            load(values);
            kernel.mergePositions(positions, new WritableChunk[] {chunk}, 0, firstRunLength,
                    values.length - firstRunLength);
            for (int ii = 0; ii < values.length; ++ii) {
                values[ii] = (Integer) chunk.get(positions.get(ii));
            }
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(chunk, payload, positions, kernel);
        }
    }

    private static IntegerKernel makeKernel(final boolean indirect, final int size,
            final Comparator<Object> comparator) {
        return indirect ? new IndirectKernel(size, comparator) : new DirectKernel(size, comparator);
    }

    private static String kernelName(final boolean indirect) {
        return indirect ? "indirect" : "direct";
    }

    private static void assertIdentity(final String description, final Integer[] values) {
        for (int ii = 0; ii < values.length; ++ii) {
            assertEquals(description + " value at " + ii, ii, values[ii].intValue());
        }
    }

    /**
     * Two adjacent ascending runs over the values [0, size) that interleave in blocks of {@code block}: the first run
     * holds the even-numbered blocks and the second the odd-numbered blocks, or the reverse when
     * {@code oddBlocksFirst}. Returns the length of the first run in {@code firstRunLength[0]}.
     */
    private static Integer[] interleavedRuns(final int size, final int block, final boolean oddBlocksFirst,
            final int[] firstRunLength) {
        final Integer[] values = new Integer[size];
        int pos = 0;
        for (final int parity : oddBlocksFirst ? new int[] {1, 0} : new int[] {0, 1}) {
            for (int ii = 0; ii < size; ++ii) {
                if ((ii / block) % 2 == parity) {
                    values[pos++] = ii;
                }
            }
            if (firstRunLength[0] == 0) {
                firstRunLength[0] = pos;
            }
        }
        return values;
    }

    /**
     * The comparisons of the two binary searches with which a merge finds where its runs overlap (the position of the
     * second run's first value in the first run, and of the first run's last value in the second run). A binary search
     * over at most size values compares at most floor(log2(size)) + 1 times.
     */
    private static long overlapSearchComparisons(final int size) {
        return 2L * (32 - Integer.numberOfLeadingZeros(size));
    }

    /** Merges the two runs of values with a fresh kernel, verifies the result, and returns the comparisons made. */
    private static long countMergeComparisons(final boolean indirect, final Integer[] values,
            final int firstRunLength) {
        final CountingComparator comparator = new CountingComparator();
        final Integer[] merged = values.clone();
        try (final IntegerKernel kernel = makeKernel(indirect, values.length, comparator)) {
            kernel.merge(merged, firstRunLength);
        }
        assertIdentity(kernelName(indirect) + " merge", merged);
        return comparator.count;
    }

    /**
     * Sorting a chunk that holds two ascending runs, one with every eighth value and one with the rest, takes one
     * linear pass to find the runs and one merge that gallops through each block of seven with no more comparisons than
     * a linear merge: at most 2(n - 1) comparisons plus the merge's two overlap searches, at every size.
     */
    @Test
    public void timsortSortOfTwoInterleavedRunsIsLinear() {
        for (final boolean indirect : new boolean[] {false, true}) {
            for (final int size : new int[] {1 << 12, 1 << 18}) {
                final Integer[] values = new Integer[size];
                int pos = 0;
                for (int ii = 0; ii < size; ii += 8) {
                    values[pos++] = ii;
                }
                for (int ii = 0; ii < size; ++ii) {
                    if (ii % 8 != 0) {
                        values[pos++] = ii;
                    }
                }
                final CountingComparator comparator = new CountingComparator();
                try (final IntegerKernel kernel = makeKernel(indirect, size, comparator)) {
                    kernel.sort(values);
                }
                assertIdentity(kernelName(indirect) + " sort", values);
                final long bound = 2L * (size - 1) + overlapSearchComparisons(size);
                assertTrue(kernelName(indirect) + " comparisons to sort " + size
                        + " values in two interleaved runs (1 of 8 / 7 of 8): " + comparator.count + " (bound "
                        + bound + ")", comparator.count <= bound);
            }
        }
    }

    /**
     * Galloping saves comparisons: past the two searches that find where its runs overlap, a merge of n values needs no
     * more than the n - 1 comparisons of a plain linear merge. Two runs that interleave in blocks of seven (the gallop
     * threshold) merge within that bound, whichever run is longer.
     */
    @Test
    public void timsortMergeOfInterleavedBlocksCostsNoMoreThanALinearMerge() {
        final int size = 1 << 16;
        for (final boolean indirect : new boolean[] {false, true}) {
            for (final boolean oddBlocksFirst : new boolean[] {false, true}) {
                final int[] firstRunLength = new int[1];
                // the longer run (even blocks) first merges from the back; the shorter run first merges from the front
                final Integer[] values = interleavedRuns(size, 7, oddBlocksFirst, firstRunLength);
                final long comparisons = countMergeComparisons(indirect, values, firstRunLength[0]);
                final long bound = size - 1 + overlapSearchComparisons(size);
                assertTrue(kernelName(indirect) + " comparisons to merge " + size
                        + " values interleaved in blocks of 7, first run length " + firstRunLength[0] + ": "
                        + comparisons + " (bound " + bound + ")", comparisons <= bound);
            }
        }
    }

    /**
     * Two runs that interleave in blocks of 100 merge by galloping through the blocks, whichever run is longer: a
     * galloping merge needs O(blocks * log(block)) comparisons (java.util TimSort uses about 0.14 per value), far fewer
     * than the n - 1 of a linear merge. Allow a quarter of a linear merge.
     */
    @Test
    public void timsortMergeGallopsThroughLongInterleavedBlocks() {
        final int size = 1 << 16;
        for (final boolean indirect : new boolean[] {false, true}) {
            for (final boolean oddBlocksFirst : new boolean[] {false, true}) {
                final int[] firstRunLength = new int[1];
                // the longer run (even blocks) first merges from the back; the shorter run first merges from the front
                final Integer[] values = interleavedRuns(size, 100, oddBlocksFirst, firstRunLength);
                final long comparisons = countMergeComparisons(indirect, values, firstRunLength[0]);
                assertTrue(kernelName(indirect) + " comparisons to merge " + size
                        + " values interleaved in blocks of 100, first run length " + firstRunLength[0] + ": "
                        + comparisons + " (bound " + size / 4 + ")", comparisons <= size / 4);
            }
        }
    }
}
