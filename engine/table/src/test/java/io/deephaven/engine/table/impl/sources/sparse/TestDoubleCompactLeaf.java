//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharCompactLeaf and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.function.IntFunction;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestDoubleCompactLeaf {

    @Test
    public void testSortedLeaf() {
        checkAgainstBlock(DoubleSortedLeaf::new);
    }

    @Test
    public void testBitmapLeaf() {
        checkAgainstBlock(DoubleBitmapLeaf::new);
    }

    private static double randomValue(final Random random) {
        return (double) random.nextInt(1000);
    }

    private static void checkValue(final String message, final double expected, final double actual) {
        if (expected != actual) {
            fail(message + ": expected " + expected + " but was " + actual);
        }
    }

    /**
     * Apply random puts, sorted puts and removes to a leaf and to a plain block, and compare them after each step.
     */
    private static void checkAgainstBlock(final IntFunction<DoubleCompactLeaf> factory) {
        final Random random = new Random(0);
        final int[] offsets = new int[BLOCK_SIZE];
        for (int trial = 0; trial < 50; ++trial) {
            final DoubleCompactLeaf leaf = factory.apply(1 + random.nextInt(8));
            final double[] expected = new double[BLOCK_SIZE];
            Arrays.fill(expected, NULL_DOUBLE);
            final boolean[] present = new boolean[BLOCK_SIZE];
            // some trials touch only a few offsets, others most of the block
            final int offsetRange = trial % 2 == 0 ? 40 : BLOCK_SIZE;
            for (int step = 0; step < 600; ++step) {
                switch (random.nextInt(5)) {
                    case 0: {
                        final int offset = random.nextInt(offsetRange);
                        leaf.remove(offset);
                        expected[offset] = NULL_DOUBLE;
                        present[offset] = false;
                        break;
                    }
                    case 1: {
                        final int first = random.nextInt(offsetRange);
                        final int last = Math.min(offsetRange - 1, first + random.nextInt(8));
                        leaf.removeRange(first, last);
                        Arrays.fill(expected, first, last + 1, NULL_DOUBLE);
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
                        try (final WritableDoubleChunk<Values> values = WritableDoubleChunk.makeWritableChunk(count + 3)) {
                            for (int ii = 0; ii < count; ++ii) {
                                final double value = randomValue(random);
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
                        final double value = randomValue(random);
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
            final DoubleCompactLeaf leaf,
            final double[] expected,
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

        final double[] copied = new double[BLOCK_SIZE];
        Arrays.fill(copied, NULL_DOUBLE);
        leaf.copyTo(copied);
        for (int offset = 0; offset < BLOCK_SIZE; ++offset) {
            checkValue("copied offset " + offset, expected[offset], copied[offset]);
        }

        try (final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(BLOCK_SIZE + 1)) {
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
        try (final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(BLOCK_SIZE + 1)) {
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
                    checkValue("overlaid offset " + offset, present[offset] ? expected[offset] : NULL_DOUBLE,
                            chunk.get(1 + offset - first));
                }
            }
        }
    }
}
