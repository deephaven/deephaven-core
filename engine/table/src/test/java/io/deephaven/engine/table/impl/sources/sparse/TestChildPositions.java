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

public class TestChildPositions {

    @Test
    public void testEmpty() {
        final ChildPositions positions = ChildPositions.empty();
        assertEquals(0, positions.size());
        assertEquals(-1, positions.lastIndex());
        assertEquals(-1, positions.find(0));
        assertEquals(-1, positions.find(BLOCK2_SIZE - 1));
        assertArrayEquals(new int[0], positions.indices());
    }

    @Test
    public void testHighestIndex() {
        // the highest leaf index is also the value of an unused sorted lane
        final ChildPositions positions = ChildPositions.of(new int[] {3, BLOCK2_SIZE - 1}, 2);
        assertEquals(1, positions.find(BLOCK2_SIZE - 1));
        assertEquals(-1, positions.find(BLOCK2_SIZE - 2));
        assertEquals(BLOCK2_SIZE - 1, positions.lastIndex());
        final ChildPositions without = ChildPositions.of(new int[] {3}, 1);
        assertEquals(-1, without.find(BLOCK2_SIZE - 1));
    }

    @Test
    public void testOfAgainstSortedArray() {
        final Random random = new Random(0);
        for (final int count : new int[] {1, 2, 4, 5, 15, 16, 17, 63, 64, 65, 1000, 4096}) {
            for (int trial = 0; trial < 5; ++trial) {
                final int[] indices = randomIndices(random, count, 0);
                check(ChildPositions.of(indices, count), indices);
            }
        }
    }

    @Test
    public void testAppendAgainstSortedArray() {
        final Random random = new Random(1);
        for (int trial = 0; trial < 20; ++trial) {
            int[] expected = new int[0];
            ChildPositions positions = ChildPositions.empty();
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
                final ChildPositions before = positions;
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

    @Test
    public void testMergeAndKeep() {
        final Random random = new Random(2);
        for (int trial = 0; trial < 50; ++trial) {
            final int[] all = randomIndices(random, 1 + random.nextInt(200), 0);
            final boolean[] isHeld = new boolean[all.length];
            final int[] heldPositions = new int[all.length];
            int heldCount = 0;
            for (int ii = 0; ii < all.length; ++ii) {
                isHeld[ii] = random.nextBoolean();
                if (isHeld[ii]) {
                    heldPositions[heldCount++] = ii;
                }
            }
            final int[] held = new int[heldCount];
            final int[] added = new int[all.length - heldCount];
            int addedCount = 0;
            for (int ii = 0, hi = 0; ii < all.length; ++ii) {
                if (isHeld[ii]) {
                    held[hi++] = all[ii];
                } else {
                    added[addedCount++] = all[ii];
                }
            }
            final int[] movedTo = new int[heldCount];
            assertArrayEquals(all, ChildPositions.merge(held, added, addedCount, movedTo));
            assertArrayEquals(Arrays.copyOf(heldPositions, heldCount), movedTo);

            final int[] kept = new int[all.length];
            final int[] keptFrom = new int[all.length];
            final int keptCount = ChildPositions.keep(all, added, addedCount, kept, keptFrom);
            assertArrayEquals(held, Arrays.copyOf(kept, keptCount));
            assertArrayEquals(Arrays.copyOf(heldPositions, heldCount), Arrays.copyOf(keptFrom, keptCount));
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

    private static void check(final ChildPositions positions, final int[] expected) {
        assertEquals(expected.length, positions.size());
        assertEquals(expected.length == 0 ? -1 : expected[expected.length - 1], positions.lastIndex());
        assertArrayEquals(expected, positions.indices());
        for (int index = 0; index < BLOCK2_SIZE; ++index) {
            final int position = Arrays.binarySearch(expected, index);
            assertEquals("index " + index, position < 0 ? -1 : position, positions.find(index));
        }
    }
}
