//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.function.IntFunction;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestCharCompactLeaf {

    @Test
    public void testSortedLeaf() {
        checkAgainstBlock(CharSortedLeaf::new);
    }

    @Test
    public void testBitmapLeaf() {
        checkAgainstBlock(CharBitmapLeaf::new);
    }

    private static char randomValue(final Random random) {
        return (char) random.nextInt(1000);
    }

    private static void checkValue(final String message, final char expected, final char actual) {
        if (expected != actual) {
            fail(message + ": expected " + expected + " but was " + actual);
        }
    }

    /**
     * Apply random puts, sorted puts and removes to a leaf and to a plain block, and compare them after each step.
     */
    private static void checkAgainstBlock(final IntFunction<CharCompactLeaf> factory) {
        final Random random = new Random(0);
        final int[] offsets = new int[BLOCK_SIZE];
        for (int trial = 0; trial < 50; ++trial) {
            final CharCompactLeaf leaf = factory.apply(1 + random.nextInt(8));
            final char[] expected = new char[BLOCK_SIZE];
            Arrays.fill(expected, NULL_CHAR);
            final boolean[] present = new boolean[BLOCK_SIZE];
            // some trials touch only a few offsets, others most of the block
            final int offsetRange = trial % 2 == 0 ? 40 : BLOCK_SIZE;
            for (int step = 0; step < 600; ++step) {
                switch (random.nextInt(5)) {
                    case 0: {
                        final int offset = random.nextInt(offsetRange);
                        leaf.remove(offset);
                        expected[offset] = NULL_CHAR;
                        present[offset] = false;
                        break;
                    }
                    case 1: {
                        final int first = random.nextInt(offsetRange);
                        final int last = Math.min(offsetRange - 1, first + random.nextInt(8));
                        leaf.removeRange(first, last);
                        Arrays.fill(expected, first, last + 1, NULL_CHAR);
                        Arrays.fill(present, first, last + 1, false);
                        break;
                    }
                    case 2: {
                        // ascending distinct offsets, some already present
                        int count = 0;
                        final int stride = 1 + random.nextInt(8);
                        for (int offset = random.nextInt(stride); offset < offsetRange && count < 24; offset +=
                                1 + random.nextInt(stride)) {
                            offsets[count++] = offset;
                        }
                        if (count == 0) {
                            break;
                        }
                        try (final WritableCharChunk<Values> values = WritableCharChunk.makeWritableChunk(count + 3)) {
                            for (int ii = 0; ii < count; ++ii) {
                                final char value = randomValue(random);
                                values.set(ii + 3, value);
                                expected[offsets[ii]] = value;
                                present[offsets[ii]] = true;
                            }
                            leaf.putSorted(offsets, values, 3, count);
                        }
                        break;
                    }
                    default: {
                        final int offset = random.nextInt(offsetRange);
                        final char value = randomValue(random);
                        leaf.put(offset, value);
                        expected[offset] = value;
                        present[offset] = true;
                    }
                }
                if (step % 50 == 0) {
                    assertMatches(leaf, expected, present, random);
                }
            }
            assertMatches(leaf, expected, present, random);
        }
    }

    private static void assertMatches(
            final CharCompactLeaf leaf,
            final char[] expected,
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

        final char[] copied = new char[BLOCK_SIZE];
        Arrays.fill(copied, NULL_CHAR);
        leaf.copyTo(copied);
        for (int offset = 0; offset < BLOCK_SIZE; ++offset) {
            checkValue("copied offset " + offset, expected[offset], copied[offset]);
        }

        try (final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(BLOCK_SIZE + 1)) {
            for (int fill = 0; fill < 10; ++fill) {
                final int first = random.nextInt(BLOCK_SIZE);
                final int last = first + random.nextInt(BLOCK_SIZE - first);
                leaf.fillRange(first, last, chunk, 1);
                for (int offset = first; offset <= last; ++offset) {
                    checkValue("filled offset " + offset, expected[offset], chunk.get(1 + offset - first));
                }
            }
        }
    }
}
