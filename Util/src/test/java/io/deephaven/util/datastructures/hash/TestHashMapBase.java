//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.hash;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class TestHashMapBase {
    private static final double[] LOAD_FACTORS = {0.5, 0.75, 0.9};

    /**
     * A put rehashes when the slot count reaches the threshold {@code (int) (entryCapacity * loadFactor)}. Verify that
     * {@link HashMapBase#capacityForExpectedEntries(int, double)} always produces a capacity whose threshold strictly
     * clears the expected count, and that it is the smallest such capacity (so we are not over-allocating).
     */
    @Test
    public void capacityForExpectedEntriesClearsThreshold() {
        final int[] expectedCounts = {
                0, 1, 2, 3, 10, 1000,
                (1 << 24) - 1, 1 << 24, (1 << 24) + 1,
                150_014_371, 200_000_000, 500_000_000, 1_000_000_000
        };
        for (final double loadFactor : LOAD_FACTORS) {
            for (final int expected : expectedCounts) {
                checkCapacity(expected, loadFactor);
            }
        }
        final Random random = new Random(12345);
        for (final double loadFactor : LOAD_FACTORS) {
            for (int ii = 0; ii < 100_000; ++ii) {
                checkCapacity(random.nextInt(Integer.MAX_VALUE), loadFactor);
            }
        }
    }

    private static void checkCapacity(final int expected, final double loadFactor) {
        final int capacity = HashMapBase.capacityForExpectedEntries(expected, loadFactor);
        final String message = String.format("loadFactor=%f, expected=%d, capacity=%d", loadFactor, expected, capacity);
        if (capacity == Integer.MAX_VALUE) {
            // No int capacity can promise this count at this load factor; the request saturates and the map instead
            // clamps to its maximum capacity, running at the nearly-full threshold.
            assertTrue(message, (int) ((Integer.MAX_VALUE - 1) * loadFactor) <= expected);
            return;
        }
        final int threshold = (int) (capacity * loadFactor);
        assertTrue(message + ", threshold=" + threshold, threshold > expected);
        // Minimality: one entry less would not have sufficed.
        assertTrue(message, (int) ((capacity - 1) * loadFactor) <= expected);
    }

    /**
     * The presized map must actually absorb the expected number of entries without rehashing.
     */
    @Test
    public void presizedMapDoesNotRehash() {
        final int[] expectedCounts = {1, 2, 10, 1000, 12345};
        for (final double loadFactor : LOAD_FACTORS) {
            for (final int expected : expectedCounts) {
                checkPresizedMapDoesNotRehash("K1V1", HashMapLockFreeK1V1.ofExpectedSize(expected, loadFactor, -1),
                        expected, loadFactor);
                checkPresizedMapDoesNotRehash("K2V2", HashMapLockFreeK2V2.ofExpectedSize(expected, loadFactor, -1),
                        expected, loadFactor);
                checkPresizedMapDoesNotRehash("K4V4", HashMapLockFreeK4V4.ofExpectedSize(expected, loadFactor, -1),
                        expected, loadFactor);
            }
        }
    }

    private static void checkPresizedMapDoesNotRehash(final String name, final NullableLongLongMap map,
            final int expected, final double loadFactor) {
        map.put(1, 1);
        final int initialCapacity = map.capacity();
        for (int ii = 2; ii <= expected; ++ii) {
            map.put(ii, ii);
        }
        assertEquals(String.format("%s: loadFactor=%f, expected=%d", name, loadFactor, expected),
                initialCapacity, map.capacity());
    }

    /**
     * Requests that no int capacity can satisfy must saturate rather than overflow or throw.
     */
    @Test
    public void hugeRequestsSaturate() {
        for (final double loadFactor : LOAD_FACTORS) {
            for (final int expected : new int[] {Integer.MAX_VALUE, Integer.MAX_VALUE - 1, 2_000_000_000}) {
                assertEquals(Integer.MAX_VALUE,
                        HashMapBase.capacityForExpectedEntries(expected, loadFactor));
            }
        }
    }

    /**
     * A saturated capacity request must round up to a positive bucket count for every bucket width; the map then clamps
     * it to its maximum capacity rather than overflowing.
     */
    @Test
    public void desiredBucketCountDoesNotOverflow() {
        for (final int entriesPerBucket : new int[] {1, 2, 4}) {
            final int buckets = HashMapBase.desiredBucketCount(Integer.MAX_VALUE, entriesPerBucket);
            final int expected = (int) (((long) Integer.MAX_VALUE + entriesPerBucket - 1) / entriesPerBucket);
            assertTrue("buckets > 0 for width " + entriesPerBucket, buckets > 0);
            assertEquals(expected, buckets);
        }
    }
}
