//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.sparse;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK2_SIZE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestLeafPositions {

    @Test
    public void testEmpty() {
        final LeafPositions positions = LeafPositions.empty();
        assertEquals(0, positions.size());
        assertEquals(-1, positions.lastIndex());
        assertEquals(-1, positions.find(0));
        assertEquals(-1, positions.find(BLOCK2_SIZE - 1));
        assertArrayEquals(new int[0], positions.indices());
    }

    @Test
    public void testHighestIndex() {
        // the highest leaf index is also the value of an unused sorted lane
        final LeafPositions positions = LeafPositions.of(new int[] {3, BLOCK2_SIZE - 1}, 2);
        assertEquals(1, positions.find(BLOCK2_SIZE - 1));
        assertEquals(-1, positions.find(BLOCK2_SIZE - 2));
        assertEquals(BLOCK2_SIZE - 1, positions.lastIndex());
        final LeafPositions without = LeafPositions.of(new int[] {3}, 1);
        assertEquals(-1, without.find(BLOCK2_SIZE - 1));
    }

    @Test
    public void testOfAgainstSortedArray() {
        final Random random = new Random(0);
        for (final int count : new int[] {1, 2, 4, 5, 15, 16, 17, 63, 64, 65, 1000, 4096}) {
            for (int trial = 0; trial < 5; ++trial) {
                final int[] indices = randomIndices(random, count, 0);
                check(LeafPositions.of(indices, count), indices);
            }
        }
    }

    @Test
    public void testAppendAgainstSortedArray() {
        final Random random = new Random(1);
        for (int trial = 0; trial < 20; ++trial) {
            int[] expected = new int[0];
            LeafPositions positions = LeafPositions.empty();
            while (expected.length < 2000) {
                // appends of one leaf at a time cross each tier boundary in place, and larger ones cross several
                final int count = random.nextBoolean() ? 1 : 1 + random.nextInt(100);
                final int from = expected.length == 0 ? 0 : expected[expected.length - 1] + 1;
                if (from + count > BLOCK2_SIZE) {
                    break;
                }
                final int[] appended = randomIndices(random, count, from, Math.min(BLOCK2_SIZE, from + 50 * count));
                final int[] previous = expected;
                expected = Arrays.copyOf(expected, expected.length + count);
                System.arraycopy(appended, 0, expected, previous.length, count);
                final LeafPositions before = positions;
                positions = positions.append(appended, count);
                check(positions, expected);
                if (positions == before) {
                    // an append in place keeps every position already held
                    for (int ii = 0; ii < previous.length; ++ii) {
                        assertEquals(ii, before.find(previous[ii]));
                    }
                }
            }
        }
    }

    private static int[] randomIndices(final Random random, final int count, final int from) {
        return randomIndices(random, count, from, BLOCK2_SIZE);
    }

    /**
     * @return {@code count} distinct ascending indices from {@code from} to {@code to - 1}
     */
    private static int[] randomIndices(final Random random, final int count, final int from, final int to) {
        final int[] indices = new int[count];
        final boolean[] chosen = new boolean[to - from];
        int found = 0;
        while (found < count) {
            final int index = random.nextInt(to - from);
            if (!chosen[index]) {
                chosen[index] = true;
                ++found;
            }
        }
        int next = 0;
        for (int ii = 0; ii < chosen.length; ++ii) {
            if (chosen[ii]) {
                indices[next++] = from + ii;
            }
        }
        return indices;
    }

    private static void check(final LeafPositions positions, final int[] expected) {
        assertEquals(expected.length, positions.size());
        assertEquals(expected.length == 0 ? -1 : expected[expected.length - 1], positions.lastIndex());
        assertArrayEquals(expected, positions.indices());
        for (int index = 0; index < BLOCK2_SIZE; ++index) {
            final int position = Arrays.binarySearch(expected, index);
            assertEquals("index " + index, position < 0 ? -1 : position, positions.find(index));
        }
    }
}
