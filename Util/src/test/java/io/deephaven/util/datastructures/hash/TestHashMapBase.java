//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.hash;

import junit.framework.TestCase;
import org.junit.Test;

public class TestHashMapBase {
    /**
     * The rehash check fires when the slot count reaches the threshold {@code (int) (entryCapacity * loadFactor)},
     * computed in float. Verify that {@link HashMapBase#capacityForExpectedEntries(long, float)} always produces a
     * capacity whose threshold strictly clears the expected count, including above float's exact-integer range (2^24)
     * where a fixed margin can be swallowed by rounding.
     */
    @Test
    public void capacityForExpectedEntriesClearsFloatThreshold() {
        final float[] loadFactors = {0.5f, 0.75f, 0.9f};
        final long[] expectedCounts = {
                0, 1, 2, 10, 1000,
                (1 << 24) - 1, 1 << 24, (1 << 24) + 1,
                150_014_371, // requests a capacity whose 0.75f threshold rounds back below it with a fixed +1 margin
                200_000_000, 500_000_000
        };
        for (final float loadFactor : loadFactors) {
            for (final long expected : expectedCounts) {
                final int capacity = HashMapBase.capacityForExpectedEntries(expected, loadFactor);
                final int threshold = (int) (capacity * loadFactor);
                final String message =
                        String.format("loadFactor=%f, expected=%d, capacity=%d, threshold=%d",
                                loadFactor, expected, capacity, threshold);
                TestCase.assertTrue(message, threshold > expected);
                // The map rounds the requested capacity up through bucket-count and prime selection; the float
                // threshold must be non-decreasing in the capacity for that rounding to preserve the guarantee.
                TestCase.assertTrue(message, (int) ((capacity + 1) * loadFactor) >= threshold);
            }
        }
    }
}
