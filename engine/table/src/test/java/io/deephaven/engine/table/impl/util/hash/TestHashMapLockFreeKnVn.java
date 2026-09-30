//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.table.impl.util.hash.NullableLongLongMaps.Shape;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class TestHashMapLockFreeKnVn {
    private static final double[] LOAD_FACTORS = {0.5, 0.75, 0.9};

    /**
     * A put rehashes when the slot count reaches the threshold {@code (int) (entryCapacity * loadFactor)}. Verify that
     * {@link HashMapLockFreeKnVn#capacityForExpectedEntries(int, double)} always produces a capacity whose threshold
     * strictly clears the expected count, and that it is the smallest such capacity (so we are not over-allocating).
     */
    @Test
    public void isMonotoneAcceptsEitherDirectionAndSamplesWideChunks() {
        assertTrue(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(new long[0])));
        assertTrue(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(new long[] {7})));
        assertTrue(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(new long[] {1, 2, 3, 10, 11, 12})));
        assertTrue(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(new long[] {12, 11, 10, 3, 2, 1})));
        // Repeats walk memory in the same direction: a sorted sample of keys is monotone for this purpose.
        assertTrue(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(new long[] {1, 1, 2, 2, 2, 3})));
        assertTrue(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(new long[] {-1, -1, -2, -2, -3})));
        // Up to 64 keys the check is exact: one turn anywhere is enough to say no.
        assertFalse(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(new long[] {1, 2, 3, 2})));
        assertFalse(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(new long[] {2, 1, 3, 4})));
        assertFalse(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(new long[] {5, 3, 4})));
        // Wide chunks are sampled every n/64th key: monotone chunks pass, a shuffled one fails, and a chunk that is
        // monotone at the sampled keys passes even if it wobbles between them (locality is what is being judged).
        final long[] wide = new long[4096];
        for (int ii = 0; ii < wide.length; ++ii) {
            wide[ii] = 1_000_000 + ii;
        }
        assertTrue(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(wide)));
        final long[] down = new long[4096];
        for (int ii = 0; ii < down.length; ++ii) {
            down[ii] = -wide[ii];
        }
        assertTrue(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(down)));
        final long[] shuffled = wide.clone();
        final java.util.Random rng = new java.util.Random(20260929);
        for (int ii = shuffled.length - 1; ii > 0; --ii) {
            final int jj = rng.nextInt(ii + 1);
            final long t = shuffled[ii];
            shuffled[ii] = shuffled[jj];
            shuffled[jj] = t;
        }
        assertFalse(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(shuffled)));
        final long[] wobbly = wide.clone();
        wobbly[1] = wobbly[2] + 1; // a turn between two sampled keys
        assertTrue(HashMapLockFreeKnVn.isMonotone(LongChunk.chunkWrap(wobbly)));
    }

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
        final int capacity = HashMapLockFreeKnVn.capacityForExpectedEntries(expected, loadFactor);
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
                for (final Shape shape : Shape.values()) {
                    checkPresizedMapDoesNotRehash(shape.name(),
                            NullableLongLongMaps.ofExpectedSize(shape, expected, loadFactor, -1), expected, loadFactor);
                }
            }
        }
    }

    private static void checkPresizedMapDoesNotRehash(final String name, final NullableLongLongMap map,
            final int expected, final double loadFactor) {
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        scalarAccess.put(1, 1);
        final int initialCapacity = map.capacity();
        for (int ii = 2; ii <= expected; ++ii) {
            scalarAccess.put(ii, ii);
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
                        HashMapLockFreeKnVn.capacityForExpectedEntries(expected, loadFactor));
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
            final int buckets = HashMapLockFreeKnVn.desiredBucketCount(Integer.MAX_VALUE, entriesPerBucket);
            final int expected = (int) (((long) Integer.MAX_VALUE + entriesPerBucket - 1) / entriesPerBucket);
            assertTrue("buckets > 0 for width " + entriesPerBucket, buckets > 0);
            assertEquals(expected, buckets);
        }
    }

    /**
     * A growing rehash doubles the entry capacity, saturating rather than overflowing: at a width's maximum the
     * doubling used to wrap negative and hand the prime finder a negative bucket count. The saturated request rounds to
     * at least the maximum bucket count for every width, which the builder then clamps.
     */
    @Test
    public void grownEntryCapacitySaturates() {
        assertEquals(2000, HashMapLockFreeKnVn.grownEntryCapacity(1000));
        assertEquals(Integer.MAX_VALUE, HashMapLockFreeKnVn.grownEntryCapacity(Integer.MAX_VALUE / 2 + 1));
        assertEquals(Integer.MAX_VALUE, HashMapLockFreeKnVn.grownEntryCapacity(Integer.MAX_VALUE));
        for (final int entriesPerBucket : new int[] {1, 2, 4}) {
            final int maxBuckets = HashMapLockFreeKnVn.getMaxBucketCapacity(entriesPerBucket);
            final int maxEntries = maxBuckets * entriesPerBucket;
            final int grown = HashMapLockFreeKnVn.grownEntryCapacity(maxEntries);
            assertTrue(grown > 0);
            assertTrue(HashMapLockFreeKnVn.desiredBucketCount(grown, entriesPerBucket) >= maxBuckets);
        }
    }

    /**
     * Every array carries its own shape: the header's tag is the bucket width, or SHAPE_TAG_EMPTY for the sentinel,
     * through allocation, growth, and reset — and the reciprocal keeps its place at the very end.
     */
    @Test
    public void headerCarriesTheShapeTag() {
        for (final Shape shape : Shape.values()) {
            final NullableLongLongMap map = NullableLongLongMaps.of(shape, 16, 0.5, -1);
            final NullableLongLongMapTestAccessors accessors = (NullableLongLongMapTestAccessors) map;
            assertTrue(shape.name(), HashMapLockFreeKnVn.isEmptyArray(accessors.keysAndValuesSnapshot()));
            assertEquals(HashMapLockFreeKnVn.SHAPE_TAG_EMPTY,
                    HashMapLockFreeKnVn.shapeTagOf(accessors.keysAndValuesSnapshot()));
            final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
            cursor.put(1, 1);
            long[] kvs = accessors.keysAndValuesSnapshot();
            checkHeader(shape, kvs);
            // Grow through several rehashes: every new array carries the tag too.
            for (long key = 2; key <= 10_000; ++key) {
                cursor.put(key, key);
            }
            assertNotSame(kvs, accessors.keysAndValuesSnapshot());
            kvs = accessors.keysAndValuesSnapshot();
            checkHeader(shape, kvs);
            map.resetToNull();
            assertTrue(shape.name(), HashMapLockFreeKnVn.isEmptyArray(accessors.keysAndValuesSnapshot()));
        }
    }

    private static void checkHeader(final Shape shape, final long[] kvs) {
        assertEquals(shape.name(), shape.bucketWidth(), HashMapLockFreeKnVn.shapeTagOf(kvs));
        assertFalse(shape.name(), HashMapLockFreeKnVn.isEmptyArray(kvs));
        final int numBuckets = (kvs.length - HashMapLockFreeKnVn.HEADER_LONGS) / (shape.bucketWidth() * 2);
        assertEquals(shape.name(), HashMapLockFreeKnVn.reciprocalFor(numBuckets),
                HashMapLockFreeKnVn.reciprocalOf(kvs));
    }
}
