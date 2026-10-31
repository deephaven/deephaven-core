//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharCompactLeaf and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.function.IntFunction;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestObjectCompactLeaf {

    @Test
    public void testSortedLeaf() {
        checkAgainstBlock(ObjectSortedLeaf::new);
    }

    @Test
    public void testBitmapLeaf() {
        checkAgainstBlock(ObjectBitmapLeaf::new);
    }

    private static Object randomValue(final Random random) {
        return (Object) random.nextInt(1000);
    }

    private static void checkValue(final String message, final Object expected, final Object actual) {
        if (expected != actual) {
            fail(message + ": expected " + expected + " but was " + actual);
        }
    }

    /**
     * Apply random puts, sorted puts and removes to a leaf and to a plain block, and compare them after each step.
     */
    private static void checkAgainstBlock(final IntFunction<ObjectCompactLeaf> factory) {
        final Random random = new Random(0);
        final int[] offsets = new int[BLOCK_SIZE];
        for (int trial = 0; trial < 50; ++trial) {
            final ObjectCompactLeaf leaf = factory.apply(1 + random.nextInt(8));
            final Object[] expected = new Object[BLOCK_SIZE];
            Arrays.fill(expected, null);
            final boolean[] present = new boolean[BLOCK_SIZE];
            // some trials touch only a few offsets, others most of the block
            final int offsetRange = trial % 2 == 0 ? 40 : BLOCK_SIZE;
            for (int step = 0; step < 600; ++step) {
                switch (random.nextInt(5)) {
                    case 0: {
                        final int offset = random.nextInt(offsetRange);
                        leaf.remove(offset);
                        expected[offset] = null;
                        present[offset] = false;
                        break;
                    }
                    case 1: {
                        final int first = random.nextInt(offsetRange);
                        final int last = Math.min(offsetRange - 1, first + random.nextInt(8));
                        leaf.removeRange(first, last);
                        Arrays.fill(expected, first, last + 1, null);
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
                        try (final WritableObjectChunk<Object, Values> values = WritableObjectChunk.makeWritableChunk(count + 3)) {
                            for (int ii = 0; ii < count; ++ii) {
                                final Object value = randomValue(random);
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
                        final Object value = randomValue(random);
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
            final ObjectCompactLeaf leaf,
            final Object[] expected,
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

        final Object[] copied = new Object[BLOCK_SIZE];
        Arrays.fill(copied, null);
        leaf.copyTo(copied);
        for (int offset = 0; offset < BLOCK_SIZE; ++offset) {
            checkValue("copied offset " + offset, expected[offset], copied[offset]);
        }

        try (final WritableObjectChunk<Object, Values> chunk = WritableObjectChunk.makeWritableChunk(BLOCK_SIZE + 1)) {
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
        try (final WritableObjectChunk<Object, Values> chunk = WritableObjectChunk.makeWritableChunk(BLOCK_SIZE + 1)) {
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
                    checkValue("overlaid offset " + offset, present[offset] ? expected[offset] : null,
                            chunk.get(1 + offset - first));
                }
            }
        }
    }
}
