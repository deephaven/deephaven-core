//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharCompactLeaf and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestLongCompactLeaf {

    @Test
    public void testSortedLeaf() {
        checkAgainstBlock(LongSortedLeaf.EMPTY, Integer.MAX_VALUE);
    }

    @Test
    public void testBitmapLeaf() {
        checkAgainstBlock(LongBitmapLeaf.EMPTY, 0);
    }

    /**
     * A sorted leaf that grows past its threshold, by any write that adds offsets, becomes a bitmap leaf.
     */
    @Test
    public void testSortedLeafBecomesBitmapLeaf() {
        checkAgainstBlock(LongSortedLeaf.EMPTY, 16);
    }

    private static long randomValue(final Random random) {
        return (long) random.nextInt(1000);
    }

    private static void checkValue(final String message, final long expected, final long actual) {
        if (expected != actual) {
            fail(message + ": expected " + expected + " but was " + actual);
        }
    }

    /**
     * Apply random puts, sorted puts, absent inserts and removes to a leaf, starting from {@code empty}, and to a plain
     * block, and compare them after each step. A step that adds or removes offsets must leave the leaf it started from
     * unchanged. A sorted leaf must hold at most {@code sortedThreshold} values, and a bitmap leaf must stay one.
     */
    private static void checkAgainstBlock(final LongCompactLeaf empty, final int sortedThreshold) {
        final Random random = new Random(0);
        final int[] offsets = new int[BLOCK_SIZE];
        for (int trial = 0; trial < 50; ++trial) {
            LongCompactLeaf leaf = empty;
            final long[] expected = new long[BLOCK_SIZE];
            Arrays.fill(expected, NULL_LONG);
            final boolean[] present = new boolean[BLOCK_SIZE];
            // some trials touch only a few offsets, others most of the block
            final int offsetRange = trial % 2 == 0 ? 40 : BLOCK_SIZE;
            for (int step = 0; step < 600; ++step) {
                final LongCompactLeaf before = leaf;
                final long[] expectedBefore = expected.clone();
                final boolean[] presentBefore = present.clone();
                switch (random.nextInt(6)) {
                    case 0: {
                        final int offset = random.nextInt(offsetRange);
                        leaf = leaf.remove(offset);
                        expected[offset] = NULL_LONG;
                        present[offset] = false;
                        break;
                    }
                    case 1: {
                        final int first = random.nextInt(offsetRange);
                        // mostly short ranges, and sometimes ranges across several bitmap words or the whole leaf
                        final int length = random.nextInt(4) == 0 ? random.nextInt(offsetRange) : random.nextInt(8);
                        final int last = Math.min(offsetRange - 1, first + length);
                        leaf = leaf.removeRange(first, last);
                        Arrays.fill(expected, first, last + 1, NULL_LONG);
                        Arrays.fill(present, first, last + 1, false);
                        break;
                    }
                    case 2: {
                        final int count = randomSortedOffsets(random, offsetRange, offsets);
                        if (count == 0) {
                            break;
                        }
                        try (final WritableLongChunk<Values> values = WritableLongChunk.makeWritableChunk(count + 3)) {
                            for (int ii = 0; ii < count; ++ii) {
                                final long value = randomValue(random);
                                values.set(ii + 3, value);
                                expected[offsets[ii]] = value;
                                present[offsets[ii]] = true;
                            }
                            leaf = leaf.putSorted(offsets, values, 3, count, sortedThreshold);
                        }
                        break;
                    }
                    case 3: {
                        // offsets already present keep their values, and the others hold null
                        final int count = randomSortedOffsets(random, offsetRange, offsets);
                        leaf = leaf.insertAbsent(offsets, count, sortedThreshold);
                        for (int ii = 0; ii < count; ++ii) {
                            present[offsets[ii]] = true;
                        }
                        break;
                    }
                    default: {
                        final int offset = random.nextInt(offsetRange);
                        final long value = randomValue(random);
                        leaf = leaf.put(offset, value, sortedThreshold);
                        expected[offset] = value;
                        present[offset] = true;
                    }
                }
                if (leaf != before) {
                    assertMatches(before, expectedBefore, presentBefore, random);
                }
                if (leaf instanceof LongSortedLeaf) {
                    assertTrue("sorted leaf of " + leaf.size(), leaf.size() <= sortedThreshold);
                }
                if (before instanceof LongBitmapLeaf) {
                    assertTrue("bitmap leaf became " + leaf.getClass(), leaf instanceof LongBitmapLeaf);
                }
                if (step % 50 == 0) {
                    assertMatches(leaf, expected, present, random);
                }
            }
            assertMatches(leaf, expected, present, random);
        }
    }

    /**
     * Fill {@code offsets} with ascending distinct offsets below {@code offsetRange}, some of which may already be
     * present in the leaf.
     *
     * @return the number of offsets
     */
    private static int randomSortedOffsets(final Random random, final int offsetRange, final int[] offsets) {
        int count = 0;
        final int stride = 1 + random.nextInt(8);
        for (int offset = random.nextInt(stride); offset < offsetRange && count < 24; offset +=
                1 + random.nextInt(stride)) {
            offsets[count++] = offset;
        }
        return count;
    }

    private static void assertMatches(
            final LongCompactLeaf leaf,
            final long[] expected,
            final boolean[] present,
            final Random random) {
        int expectedSize = 0;
        for (int offset = 0; offset < BLOCK_SIZE; ++offset) {
            checkValue("offset " + offset, expected[offset], leaf.get(offset));
            assertEquals("offset " + offset, present[offset], leaf.contains(offset));
            if (present[offset]) {
                ++expectedSize;
            }
        }
        assertEquals(expectedSize, leaf.size());

        final long[] copied = new long[BLOCK_SIZE];
        Arrays.fill(copied, NULL_LONG);
        leaf.copyTo(copied);
        for (int offset = 0; offset < BLOCK_SIZE; ++offset) {
            checkValue("copied offset " + offset, expected[offset], copied[offset]);
        }

        try (final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(BLOCK_SIZE + 1)) {
            for (int fill = 0; fill < 10; ++fill) {
                final int first = random.nextInt(BLOCK_SIZE);
                final int last = first + random.nextInt(BLOCK_SIZE - first);
                leaf.fillRange(first, last, chunk, 1);
                for (int offset = first; offset <= last; ++offset) {
                    checkValue("filled offset " + offset, expected[offset], chunk.get(1 + offset - first));
                }
            }
        }

        final int[] offsets = new int[BLOCK_SIZE];
        try (final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(BLOCK_SIZE + 1)) {
            for (int fill = 0; fill < 10; ++fill) {
                final int first = random.nextInt(BLOCK_SIZE);
                final int last = first + random.nextInt(BLOCK_SIZE - first);
                final int count = leaf.copyOffsets(first, last, offsets);
                int expectedCount = 0;
                for (int offset = first; offset <= last; ++offset) {
                    if (present[offset]) {
                        assertEquals("copied offset", offset, offsets[expectedCount++]);
                    }
                }
                assertEquals(expectedCount, count);

                chunk.fillWithNullValue(0, BLOCK_SIZE + 1);
                leaf.overlayRange(first, last, chunk, 1);
                for (int offset = first; offset <= last; ++offset) {
                    checkValue("overlaid offset " + offset, present[offset] ? expected[offset] : NULL_LONG,
                            chunk.get(1 + offset - first));
                }
            }
        }
    }
}
