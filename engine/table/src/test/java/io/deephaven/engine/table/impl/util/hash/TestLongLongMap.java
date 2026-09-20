//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.util.mutable.MutableInt;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongLongBiConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.LongUnaryOperator;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestLongLongMap {
    private static final Factory referenceFactory = new Factory("fastutil", 1, TestLongLongMap::newReferenceMap);

    private static NullableLongLongMap newReferenceMap(final int initialCapacity, final float loadFactor) {
        return new TestNullableLongLongMap(initialCapacity, loadFactor);
    }

    @Parameterized.Parameters(name = "map={0}, cap={1}, load={2}")
    public static Iterable<Object[]> data() {
        List<Object[]> result = new ArrayList<>();
        final Factory[] factories = {
                referenceFactory,
                new Factory("K1V1", 1, HashMapLockFreeK1V1::new),
                new Factory("K2V2", 2, HashMapLockFreeK2V2::new),
                new Factory("K4V4", 4, HashMapLockFreeK4V4::new)
        };
        final int[] initialCapacities = {10, 1000, 1000000};
        final float[] loadFactors = {0.5f, 0.75f, 0.9f};
        for (Factory factory : factories) {
            for (int ic : initialCapacities) {
                for (float lf : loadFactors) {
                    result.add(new Object[] {factory, ic, lf});
                }
            }
        }
        return result;
    }

    private final Factory factory;
    private final int initialCapacity;
    private final float loadFactor;

    public TestLongLongMap(Factory factory, int initialCapacity, float loadFactor) {
        this.factory = factory;
        this.initialCapacity = initialCapacity;
        this.loadFactor = loadFactor;
    }

    @Test
    public void zeroKey() {
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        scalarAccess.put(0, 12345);
        assertEquals(scalarAccess.get(0), 12345);
        assertEquals(map.size(), 1);
    }

    @Test
    public void badKeys() {
        // The reference fastutil implementation doesn't have key limitations
        if (factory == referenceFactory) {
            return;
        }
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        try {
            scalarAccess.put(HashMapBase.SPECIAL_KEY_FOR_DELETED_SLOT, 12345);
            fail("SPECIAL_KEY_FOR_DELETED_SLOT should not be accepted");
        } catch (io.deephaven.base.verify.AssertionFailure e) {
            // do nothing
        }
        try {
            scalarAccess.put(HashMapBase.REDIRECTED_KEY_FOR_EMPTY_SLOT, 12345);
            fail("REDIRECTED_KEY_FOR_EMPTY_SLOT should not be accepted");
        } catch (io.deephaven.base.verify.AssertionFailure e) {
            // do nothing
        }
    }

    @Test
    public void nullMapReturnsNoEntry() {
        // The reference fastutil implementation doesn't have resetToNull
        if (factory == referenceFactory) {
            return;
        }
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.defaultReturnValue();
        // The hoisted ScalarAccess pattern: allocate and reset a cursor once, outside your loops; the cursor's own
        // operations are then cheap, and its mutators keep its binding fresh. Reset again only after the map is
        // mutated other than through the cursor. (Code whose enclosing method is itself invoked per-element has no
        // loop to hoist over — stash the cursor in a ThreadLocal instead; see WritableRowRedirectionLockFree.)
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        scalarAccess.put(0, 1);
        scalarAccess.put(2, 3);
        map.resetToNull();
        // resetToNull() is not a cursor operation: reset the invalidated binding.
        scalarAccess.reset(map);
        for (int ii = 0; ii < 4; ++ii) {
            assertEquals(scalarAccess.get(ii), noEntryValue);
        }
    }

    @Test
    public void prevValuesOnInsert() {
        final long beginKey = 100;
        final long endKey = 200;
        final long beginValue = 5000;
        final long endValue = 5010;
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.defaultReturnValue();
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (long valueBase = beginValue; valueBase < endValue; ++valueBase) {
            for (long key = beginKey; key < endKey; ++key) {
                final long expectedPrevious = valueBase == beginValue ? noEntryValue : key + valueBase - 1;
                final long actualPrevious = scalarAccess.put(key, key + valueBase);
                assertEquals(expectedPrevious, actualPrevious);
            }
        }
    }

    @Test
    public void prevValuesOnRemove() {
        final long beginKey = 100;
        final long endKey = 200;
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.defaultReturnValue();
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (long key = beginKey; key < endKey; ++key) {
            final long previous = scalarAccess.put(key, key - 10000);
            assertEquals(previous, noEntryValue);
        }
        for (long key = beginKey; key < endKey; ++key) {
            final long expectedPrevious = key - 10000;
            final long actualPrevious = scalarAccess.remove(key);
            assertEquals(expectedPrevious, actualPrevious);
        }
    }

    @Test
    public void putIfAbsent() {
        final long beginKey = 100;
        final long endKey = 200;
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.defaultReturnValue();
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (long key = beginKey; key < endKey; key += 2) {
            final long previous = scalarAccess.put(key, key + 5000);
            assertEquals(previous, noEntryValue);
        }
        for (long key = beginKey; key < endKey; ++key) {
            final long expectedPrevious = (key % 2) == 0 ? key + 5000 : noEntryValue;
            final long actualPrevious = scalarAccess.putIfAbsent(key, key + 10000);
            assertEquals(expectedPrevious, actualPrevious);
        }
        // The cursor did all the mutating itself, so its binding is still fresh for the reads.
        for (long key = beginKey; key < endKey; ++key) {
            final long expectedValue = (key % 2) == 0 ? key + 5000 : key + 10000;
            final long actualValue = scalarAccess.get(key);
            assertEquals(expectedValue, actualValue);
        }
    }

    @Test
    public void clear() {
        final int numIterations = 10;
        final int sizeAtWhichToClear = 10000;
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (int iteration = 0; iteration < numIterations; ++iteration) {
            assertEquals(0, map.size());
            for (long ii = 0; ii < sizeAtWhichToClear; ++ii) {
                scalarAccess.put(ii, ii + 1);
            }
            assertEquals(sizeAtWhichToClear, map.size());
            map.clear();
            // clear() is not a cursor operation: reset the invalidated binding.
            scalarAccess.reset(map);
        }
    }

    @Test
    public void setToNull() {
        // The reference fastutil implementation doesn't have resetToNull
        if (factory == referenceFactory) {
            return;
        }
        final int numIterations = 10;
        final int sizeAtWhichToClear = 10000;
        NullableLongLongMap map = (NullableLongLongMap) factory.create(initialCapacity, loadFactor);
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (int iteration = 0; iteration < numIterations; ++iteration) {
            assertEquals(0, map.size());
            for (long ii = 0; ii < sizeAtWhichToClear; ++ii) {
                scalarAccess.put(ii, ii + 1);
            }
            assertEquals(sizeAtWhichToClear, map.size());
            map.resetToNull();
            // resetToNull() is not a cursor operation: reset the invalidated binding.
            scalarAccess.reset(map);
        }
    }

    /**
     * An insert whose probe starts in a bucket holding a deleted slot ahead of an empty one takes the deleted slot — in
     * the first bucket of the probe as in every later one — so putting removed keys back does not consume empty slots
     * and walk the map toward a needless rehash. (K1V1 has one slot per bucket and was always right; the unrolled first
     * bucket of K2V2 and K4V4 used to take the empty slot instead.)
     */
    @Test
    public void firstBucketReusesTombstones() {
        // The reference fastutil implementation is a different map altogether.
        if (factory == referenceFactory) {
            return;
        }
        // Every arrangement of the first bucket an insert can meet: keys in slots 0..occupied-1, one or two of them
        // deleted, and either an empty slot after them or, when the bucket is full, the probe moving on to the next
        // bucket. The insert must take the earliest deleted slot, never the empty one.
        final int entriesPerBucket = factory.getEntriesPerBucket();
        for (int occupied = 1; occupied <= entriesPerBucket; ++occupied) {
            for (int deleted = 0; deleted < occupied; ++deleted) {
                checkTombstoneReuse(occupied, deleted, -1);
                for (int alsoDeleted = deleted + 2; alsoDeleted < occupied; ++alsoDeleted) {
                    checkTombstoneReuse(occupied, deleted, alsoDeleted);
                }
            }
        }
    }

    /**
     * Fill the first bucket of a fresh map with {@code occupied} colliding keys, delete the one at {@code deleted} (and
     * the one at {@code alsoDeleted}, when it is not -1), insert one more colliding key, and check that it took the
     * earliest tombstone: the count of non-empty slots is unchanged and the new key stands where the deleted key stood.
     * (The two tombstones are never adjacent, so the key array tells the right slot from the wrong one.)
     */
    private void checkTombstoneReuse(final int occupied, final int deleted, final int alsoDeleted) {
        final String where = "occupied=" + occupied + " deleted=" + deleted + " alsoDeleted=" + alsoDeleted;
        final NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final HashMapBase base = (HashMapBase) map;
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        // The first key goes in before anything is measured: a never-populated map has no array, hence no capacity.
        final long first = 1;
        cursor.put(first, 10);
        final int numBuckets = map.capacity() / factory.getEntriesPerBucket();
        final int bucket = HashMapBase.probe1(first, numBuckets);
        // occupied + 1 keys whose probes all start in that bucket, in the order they will be inserted
        final long[] colliding = new long[occupied + 1];
        colliding[0] = first;
        long candidate = first;
        for (int ci = 1; ci < colliding.length; ++ci) {
            do {
                ++candidate;
            } while (HashMapBase.probe1(candidate, numBuckets) != bucket);
            colliding[ci] = candidate;
        }
        for (int ki = 1; ki < occupied; ++ki) {
            cursor.put(colliding[ki], 10 + ki);
        }
        assertEquals(where, occupied, base.nonEmptySlots);
        cursor.remove(colliding[deleted]);
        if (alsoDeleted != -1) {
            cursor.remove(colliding[alsoDeleted]);
        }
        // Tombstones still count as non-empty.
        assertEquals(where, occupied, base.nonEmptySlots);
        final long fresh = colliding[occupied];
        // remove() does not go through the cursor yet: reset the invalidated binding.
        cursor.reset(map);
        cursor.put(fresh, 99);
        assertEquals(where, occupied, base.nonEmptySlots);
        // The keys in slot order: the fresh key stands where the earliest deleted key stood.
        final long[] expected = new long[occupied - (alsoDeleted == -1 ? 0 : 1)];
        int ei = 0;
        for (int ki = 0; ki < occupied; ++ki) {
            if (ki == deleted) {
                expected[ei++] = fresh;
            } else if (ki != alsoDeleted) {
                expected[ei++] = colliding[ki];
            }
        }
        assertArrayEquals(where, expected, ((NullableLongLongMapTestAccessors) map).keyArray());
    }

    /**
     * The same rule in the buckets after the first: a probe that leaves a full first bucket and meets a tombstone ahead
     * of an empty slot in a later bucket must take the tombstone. (The loop remembered a bucket's tombstones only after
     * scanning the whole bucket, so a tombstone followed by an empty slot in the same later bucket lost to the empty
     * slot.)
     */
    @Test
    public void laterBucketReusesTombstones() {
        if (factory == referenceFactory) {
            return;
        }
        final int entriesPerBucket = factory.getEntriesPerBucket();
        for (int filled = 1; filled < entriesPerBucket; ++filled) {
            for (int deleted = 0; deleted < filled; ++deleted) {
                checkLaterBucketTombstoneReuse(filled, deleted);
            }
        }
    }

    /**
     * Fill a key's first bucket with other keys so its probe moves on, put {@code filled} keys whose first bucket is
     * that key's second bucket, delete the one at {@code deleted}, then insert the key: it must take the tombstone.
     */
    private void checkLaterBucketTombstoneReuse(final int filled, final int deleted) {
        final String where = "filled=" + filled + " deleted=" + deleted;
        final int entriesPerBucket = factory.getEntriesPerBucket();
        // Room for every key of the test without a rehash, whatever the parameterized capacity.
        final NullableLongLongMap map = factory.create(1000, loadFactor);
        final HashMapBase base = (HashMapBase) map;
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        final long first = 1;
        cursor.put(first, 10);
        final int numBuckets = map.capacity() / entriesPerBucket;
        final int bucket = HashMapBase.probe1(first, numBuckets);
        // Fill the first bucket: entriesPerBucket keys whose probes start there, then one more, the key under test.
        long candidate = first;
        for (int ki = 1; ki < entriesPerBucket; ++ki) {
            do {
                ++candidate;
            } while (HashMapBase.probe1(candidate, numBuckets) != bucket);
            cursor.put(candidate, 10 + ki);
        }
        do {
            ++candidate;
        } while (HashMapBase.probe1(candidate, numBuckets) != bucket);
        final long key = candidate;
        // Its second bucket, as the probe loop computes it: one plus the second hash, in buckets, past the first.
        final int secondBucket = (bucket + 1 + HashMapBase.probe2(key, numBuckets - 2)) % numBuckets;
        // filled keys whose first bucket is that second bucket; they take its slots 0..filled-1 in order.
        final long[] others = new long[filled];
        candidate = 1_000_000;
        for (int ki = 0; ki < filled; ++ki) {
            do {
                ++candidate;
            } while (HashMapBase.probe1(candidate, numBuckets) != secondBucket);
            others[ki] = candidate;
            cursor.put(candidate, 100 + ki);
        }
        assertEquals(where, entriesPerBucket + filled, base.nonEmptySlots);
        cursor.remove(others[deleted]);
        assertEquals(where, entriesPerBucket + filled, base.nonEmptySlots);
        // remove() does not go through the cursor yet: reset the invalidated binding.
        cursor.reset(map);
        cursor.put(key, 99);
        // The tombstone was reused: the count of non-empty slots did not grow, and the map holds what it should.
        assertEquals(where, entriesPerBucket + filled, base.nonEmptySlots);
        assertEquals(where, entriesPerBucket + filled, map.size());
        assertEquals(where, 99, cursor.get(key));
    }

    /**
     * A map that has never been populated — or has been reset to null — is empty, not broken: clearing it is a no-op
     * and its key and value accessors answer with nothing, where they used to dereference the array it does not have.
     */
    @Test
    public void neverPopulatedMapIsEmptyNotBroken() {
        // The reference fastutil implementation always has storage.
        if (factory == referenceFactory) {
            return;
        }
        final NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        // Never populated: there is no array yet.
        assertEmptyNotBroken(map);
        // Populate it, so that the reset below releases a real array, then check the same things once more.
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        cursor.put(1, 10);
        cursor.put(2, 20);
        cursor.put(3, 30);
        assertEquals(3, map.size());
        assertTrue(map.capacity() > 0);
        map.resetToNull();
        assertEmptyNotBroken(map);
    }

    /**
     * Clearing a map without an array is a no-op, and its size, capacity and accessors all answer with nothing.
     */
    private static void assertEmptyNotBroken(final NullableLongLongMap map) {
        final NullableLongLongMapTestAccessors accessors = (NullableLongLongMapTestAccessors) map;
        map.clear();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertEquals(0, map.capacity());
        assertEquals(0, accessors.keyArray().length);
        assertEquals(0, accessors.valueArray().length);
        final long[] space = new long[4];
        assertSame(space, accessors.keyArray(space));
        assertSame(space, accessors.valueArray(space));
    }

    @Test
    public void zeroComesBackThroughKeys() {
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long specialKey = HashMapBase.SPECIAL_KEY_FOR_EMPTY_SLOT;
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        scalarAccess.put(specialKey, 12345);
        final long[] keys = ((NullableLongLongMapTestAccessors) map).keyArray();
        assertEquals(1, keys.length);
        assertEquals(specialKey, keys[0]);
    }

    @Test
    public void testKeysAndValues() {
        Map<Long, Long> reference = new HashMap<>(initialCapacity, loadFactor);
        NullableLongLongMapTestAccessors test =
                (NullableLongLongMapTestAccessors) factory.create(initialCapacity, loadFactor);
        Random rng = new Random(1283712890);
        populate(rng, 1000000, 10000, 0.75, reference, test);

        final long[] expectedKeys = new long[reference.size()];
        final long[] expectedValues = new long[reference.size()];
        int nextIndex = 0;
        for (Map.Entry<Long, Long> entry : reference.entrySet()) {
            expectedKeys[nextIndex] = entry.getKey();
            expectedValues[nextIndex] = entry.getValue();
            ++nextIndex;
        }
        assertEquals(nextIndex, reference.size());
        assertEquals(reference.size(), test.size());

        final long[] actualKeys = test.keyArray();
        final long[] actualValues = test.valueArray();
        assertEquals(expectedKeys.length, actualKeys.length);
        assertEquals(expectedValues.length, actualValues.length);

        Arrays.sort(expectedKeys);
        Arrays.sort(expectedValues);
        Arrays.sort(actualKeys);
        Arrays.sort(actualValues);

        assertArrayEquals(expectedKeys, actualKeys);
        assertArrayEquals(expectedValues, actualValues);

        if (test instanceof HashMapBase) {
            // Also exercise the caller-provided-space overloads.
            final long[] keySpace = new long[reference.size()];
            final long[] valueSpace = new long[reference.size()];
            assertSame(keySpace, test.keyArray(keySpace));
            assertSame(valueSpace, test.valueArray(valueSpace));
            Arrays.sort(keySpace);
            Arrays.sort(valueSpace);
            assertArrayEquals(expectedKeys, keySpace);
            assertArrayEquals(expectedValues, valueSpace);
        }
    }

    @Test
    public void do100KInserts() {
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long beginKey = -50000;
        final long endKey = 50000;
        final long size = endKey - beginKey;
        final long noEntryValue = map.defaultReturnValue();
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (long key = beginKey; key < endKey; ++key) {
            scalarAccess.put(key, key + 1000000);
        }
        assertEquals(map.size(), size);
        // The cursor did all the mutating itself, so one binding serves the fills and all three read loops.
        // These lookups should fail
        for (long key = beginKey - size; key < beginKey; ++key) {
            final long result = scalarAccess.get(key);
            assertEquals(result, noEntryValue);
        }
        // These lookups should succeed
        for (long key = beginKey; key < endKey; ++key) {
            final long result = scalarAccess.get(key);
            assertEquals(result, key + 1000000);
        }
        // These lookups should fail
        for (long key = endKey; key < endKey + size; ++key) {
            final long result = scalarAccess.get(key);
            assertEquals(result, noEntryValue);
        }
    }

    @Test
    public void do100KInsertsThen50KRemoves() {
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long beginKey = 0;
        final long endKey = 100000;
        final long size = endKey - beginKey;
        final long noEntryValue = map.defaultReturnValue();
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (long key = beginKey; key < endKey; ++key) {
            scalarAccess.put(key, key + 1000000);
        }
        for (long key = beginKey; key < endKey; key += 2) {
            scalarAccess.remove(key);
        }
        assertEquals(map.size(), size / 2);
        // The cursor did all the mutating itself, so its binding is still fresh for the reads.
        for (long key = beginKey; key < endKey; ++key) {
            final long expectedResult = (key % 2) == 0 ? noEntryValue : key + 1000000;
            final long actualResult = scalarAccess.get(key);
            assertEquals(expectedResult, actualResult);
        }
    }

    @Test
    public void chunkedGetHitsAndMisses() {
        final NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.defaultReturnValue();
        final long beginKey = -50000;
        final long endKey = 50000;
        final long size = endKey - beginKey;
        final int totalProbes = (int) (3 * size);
        final long[] probes = new long[totalProbes];
        final long probeBegin = beginKey - size;
        for (int ii = 0; ii < totalProbes; ++ii) {
            probes[ii] = probeBegin + ii;
        }

        // A chunked get on a never-populated map yields noEntryValue everywhere.
        checkChunkedGet(map, probes, 4096, key -> noEntryValue);

        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (long key = beginKey; key < endKey; ++key) {
            scalarAccess.put(key, key + 1000000);
        }

        // Probe a range three times as wide as the occupied keyspace — misses below, hits, misses above — through
        // the chunked entry point, with chunk sizes covering the degenerate, the odd, the typical (with a partial
        // tail), and everything-in-one-chunk.
        for (final int chunkSize : new int[] {1, 7, 4096, totalProbes}) {
            checkChunkedGet(map, probes, chunkSize,
                    key -> key >= beginKey && key < endKey ? key + 1000000 : noEntryValue);
        }

        // An empty keys chunk yields an empty result (the result-size contract).
        final WritableLongChunk<Any> emptyResult = WritableLongChunk.writableChunkWrap(new long[1]);
        map.get(LongChunk.chunkWrap(new long[0]), emptyResult);
        assertEquals(0, emptyResult.size());
    }

    @Test
    public void chunkedGetAfterRemoves() {
        final NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.defaultReturnValue();
        final long endKey = 100000;
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < endKey; ++key) {
            scalarAccess.put(key, key + 1000000);
        }
        for (long key = 0; key < endKey; key += 2) {
            scalarAccess.remove(key);
        }
        // Even keys are tombstoned; the chunked path must probe past the tombstones exactly as a scalar get would.
        final long[] probes = new long[(int) endKey];
        for (int ii = 0; ii < probes.length; ++ii) {
            probes[ii] = ii;
        }
        for (final int chunkSize : new int[] {1000, 4096}) {
            checkChunkedGet(map, probes, chunkSize, key -> (key % 2) == 0 ? noEntryValue : key + 1000000);
        }
    }

    /**
     * Feed {@code probes} through the chunked get in slices of at most {@code chunkSize}, checking every result and the
     * result-size contract on each call.
     */
    private static void checkChunkedGet(final NullableLongLongMap map, final long[] probes, final int chunkSize,
            final LongUnaryOperator expected) {
        final WritableLongChunk<Any> resultChunk = WritableLongChunk.writableChunkWrap(new long[chunkSize]);
        for (int begin = 0; begin < probes.length; begin += chunkSize) {
            final int thisSize = Math.min(chunkSize, probes.length - begin);
            map.get(LongChunk.chunkWrap(probes, begin, thisSize), resultChunk);
            assertEquals(thisSize, resultChunk.size());
            for (int ii = 0; ii < thisSize; ++ii) {
                assertEquals(expected.applyAsLong(probes[begin + ii]), resultChunk.get(ii));
            }
        }
    }

    /**
     * The chunked put and putIfAbsent called directly, in slices of several sizes: the old-value chunk carries one
     * entry per key and is sized by the call; a key that appears twice in a batch is processed in index order, so put's
     * second element overwrites the first and reports its value as the old one, while putIfAbsent's second element
     * keeps the first and reports it; and a batch far larger than the map's capacity rehashes several times inside one
     * call without losing an element. A second pass over the same keys then finds every key present. java.util.HashMap,
     * fed the same elements in the same order, is the standard of correctness.
     */
    @Test
    public void chunkedPutAndPutIfAbsent() {
        final int distinct = 5000;
        final long[] keys = new long[2 * distinct];
        final long[] values = new long[keys.length];
        for (int ii = 0; ii < distinct; ++ii) {
            final long key = 1_000_003L * ii + 17;
            keys[2 * ii] = key;
            values[2 * ii] = 1_000_000 + ii;
            keys[2 * ii + 1] = key;
            values[2 * ii + 1] = 2_000_000 + ii;
        }
        final long[] laterValues = new long[values.length];
        for (int ii = 0; ii < values.length; ++ii) {
            laterValues[ii] = values[ii] + 5;
        }
        for (final int chunkSize : new int[] {1, 7, 4096, keys.length}) {
            for (final boolean ifAbsent : new boolean[] {false, true}) {
                // A fresh map at the parameterized capacity (10 at the smallest), so the whole-batch slice rehashes
                // repeatedly mid-call.
                final NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
                final Map<Long, Long> reference = new HashMap<>();
                checkChunkedPut(map, keys, values, chunkSize, ifAbsent, reference);
                checkChunkedPut(map, keys, laterValues, chunkSize, ifAbsent, reference);
            }
        }
    }

    /**
     * Feed {@code keys} and {@code values} through the chunked put (or putIfAbsent) in slices of at most
     * {@code chunkSize}, checking the old-value and size contract of every call against {@code reference}, which
     * receives the same elements in the same order; then check that the map holds exactly what the reference holds.
     */
    private static void checkChunkedPut(final NullableLongLongMap map, final long[] keys, final long[] values,
            final int chunkSize, final boolean ifAbsent, final Map<Long, Long> reference) {
        final long noEntryValue = map.defaultReturnValue();
        final WritableLongChunk<Any> oldValues = WritableLongChunk.writableChunkWrap(new long[chunkSize]);
        for (int begin = 0; begin < keys.length; begin += chunkSize) {
            final int thisSize = Math.min(chunkSize, keys.length - begin);
            final LongChunk<Any> keyChunk = LongChunk.chunkWrap(keys, begin, thisSize);
            final LongChunk<Any> valueChunk = LongChunk.chunkWrap(values, begin, thisSize);
            // The call sizes the output; start it empty so that a call that forgot would be caught.
            oldValues.setSize(0);
            if (ifAbsent) {
                map.putIfAbsent(keyChunk, valueChunk, oldValues);
            } else {
                map.put(keyChunk, valueChunk, oldValues);
            }
            assertEquals(thisSize, oldValues.size());
            for (int ii = 0; ii < thisSize; ++ii) {
                final Long expectedOld = ifAbsent
                        ? reference.putIfAbsent(keys[begin + ii], values[begin + ii])
                        : reference.put(keys[begin + ii], values[begin + ii]);
                assertEquals(expectedOld == null ? noEntryValue : expectedOld, oldValues.get(ii));
            }
        }
        checkAgainstReference(map, reference);
    }

    /**
     * The two puts that report no old values, in slices of several sizes: the pair form writes each key's own value and
     * the one-value form writes the same value under every key; both overwrite what is there, take a duplicate key in
     * index order, and rehash mid-call like the reporting form. java.util.HashMap, fed the same elements in the same
     * order, is the standard of correctness.
     */
    @Test
    public void chunkedPutWithoutOldValues() {
        final int distinct = 5000;
        final long[] keys = new long[2 * distinct];
        final long[] values = new long[keys.length];
        for (int ii = 0; ii < distinct; ++ii) {
            final long key = 1_000_003L * ii + 17;
            keys[2 * ii] = key;
            values[2 * ii] = 1_000_000 + ii;
            keys[2 * ii + 1] = key;
            values[2 * ii + 1] = 2_000_000 + ii;
        }
        for (final int chunkSize : new int[] {1, 7, 4096, keys.length}) {
            final NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
            final Map<Long, Long> reference = new HashMap<>();
            for (int begin = 0; begin < keys.length; begin += chunkSize) {
                final int thisSize = Math.min(chunkSize, keys.length - begin);
                map.put(LongChunk.chunkWrap(keys, begin, thisSize), LongChunk.chunkWrap(values, begin, thisSize));
                for (int ii = 0; ii < thisSize; ++ii) {
                    reference.put(keys[begin + ii], values[begin + ii]);
                }
            }
            checkAgainstReference(map, reference);
            // Then the one-value form over the same keys, overwriting every entry.
            for (int begin = 0; begin < keys.length; begin += chunkSize) {
                final int thisSize = Math.min(chunkSize, keys.length - begin);
                map.put(LongChunk.chunkWrap(keys, begin, thisSize), 77);
                for (int ii = 0; ii < thisSize; ++ii) {
                    reference.put(keys[begin + ii], 77L);
                }
            }
            checkAgainstReference(map, reference);
        }
    }

    /** The map holds exactly the reference's entries: the same size, and every reference key reads back its value. */
    private static void checkAgainstReference(final NullableLongLongMap map, final Map<Long, Long> reference) {
        assertEquals(reference.size(), map.size());
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        for (final Map.Entry<Long, Long> entry : reference.entrySet()) {
            assertEquals((long) entry.getValue(), cursor.get(entry.getKey()));
        }
    }

    @Test
    public void chunkedRemoveHitsAndMisses() {
        final NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.defaultReturnValue();
        final long beginKey = -50000;
        final long endKey = 50000;
        final long size = endKey - beginKey;
        // Probes cover a range three times as wide as the occupied keyspace: misses below, hits, misses above.
        final int totalProbes = (int) (3 * size);
        final long[] probes = new long[totalProbes];
        final long probeBegin = beginKey - size;
        for (int ii = 0; ii < totalProbes; ++ii) {
            probes[ii] = probeBegin + ii;
        }

        // A chunked remove on a never-populated map yields noEntryValue everywhere and removes nothing.
        checkChunkedRemove(map, probes, 4096, key -> noEntryValue);
        assertEquals(0, map.size());

        // Fill, then remove everything through the chunked entry point, with chunk sizes covering the degenerate,
        // the odd, the typical (with a partial tail), and everything-in-one-chunk.
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (final int chunkSize : new int[] {1, 7, 4096, totalProbes}) {
            // The chunked removes of the previous iteration did not go through the cursor: reset the invalidated
            // binding.
            scalarAccess.reset(map);
            for (long key = beginKey; key < endKey; ++key) {
                scalarAccess.put(key, key + 1000000);
            }
            checkChunkedRemove(map, probes, chunkSize,
                    key -> key >= beginKey && key < endKey ? key + 1000000 : noEntryValue);
            assertEquals(0, map.size());
        }

        // An empty keys chunk yields an empty result (the result-size contract).
        final WritableLongChunk<Any> emptyResult = WritableLongChunk.writableChunkWrap(new long[1]);
        map.remove(LongChunk.chunkWrap(new long[0]), emptyResult);
        assertEquals(0, emptyResult.size());
    }

    @Test
    public void chunkedRemoveSeesOwnEarlierRemoves() {
        final NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.defaultReturnValue();
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < 10; ++key) {
            scalarAccess.put(key, key + 1000000);
        }
        // Elements are processed in index order, so the duplicates of key 5 find nothing left to remove.
        final long[] keys = {5, 5, 7, 5};
        final long[] expected = {5 + 1000000, noEntryValue, 7 + 1000000, noEntryValue};
        final WritableLongChunk<Any> oldValues = WritableLongChunk.writableChunkWrap(new long[keys.length]);
        map.remove(LongChunk.chunkWrap(keys), oldValues);
        assertEquals(keys.length, oldValues.size());
        for (int ii = 0; ii < keys.length; ++ii) {
            assertEquals(expected[ii], oldValues.get(ii));
        }
        assertEquals(8, map.size());
    }

    /**
     * Feed {@code probes} through the chunked remove in slices of at most {@code chunkSize}, checking every returned
     * old value and the result-size contract on each call.
     */
    private static void checkChunkedRemove(final NullableLongLongMap map, final long[] probes, final int chunkSize,
            final LongUnaryOperator expected) {
        final WritableLongChunk<Any> oldValuesChunk = WritableLongChunk.writableChunkWrap(new long[chunkSize]);
        for (int begin = 0; begin < probes.length; begin += chunkSize) {
            final int thisSize = Math.min(chunkSize, probes.length - begin);
            map.remove(LongChunk.chunkWrap(probes, begin, thisSize), oldValuesChunk);
            assertEquals(thisSize, oldValuesChunk.size());
            for (int ii = 0; ii < thisSize; ++ii) {
                assertEquals(expected.applyAsLong(probes[begin + ii]), oldValuesChunk.get(ii));
            }
        }
    }

    @Test
    public void do1MRandomOperationsLotsOfCollisions() {
        // Standard of correctness: java.util.HashMap
        Map<Long, Long> reference = new HashMap<>(initialCapacity, loadFactor);
        NullableLongLongMap test = factory.create(initialCapacity, loadFactor);
        Random rng = new Random(12345);
        populate(rng, 1000000, 10000, 0.75, reference, test);

        assertEquals(reference.size(), test.size());

        Entries masterEntries = Entries.create(reference);
        Entries targetEntries = Entries.create(test);

        assertTrue(masterEntries.destructivelyEquals(targetEntries));
    }

    @Test
    public void mapStaysSmall() {
        // no way to ask the reference fastutil map for its capacity
        if (factory == referenceFactory) {
            return;
        }
        final int size = 1000;
        final int iterations = 1000000;
        final long randomMod = 1000000000; // 1 billion
        // Use this interface because we want to access 'capacity'
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        Random insertStream = new Random(67890);
        Random deleteStream = new Random(67890);

        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        for (int ii = 0; ii < size; ++ii) {
            final long key = insertStream.nextLong() % randomMod;
            final long value = key + 12;
            scalarAccess.put(key, value);
        }

        for (int ii = 0; ii < iterations; ++ii) {
            final long deleteKey = deleteStream.nextLong() % randomMod;
            scalarAccess.remove(deleteKey);

            final long key = insertStream.nextLong() % randomMod;
            final long value = key + 12;
            scalarAccess.put(key, value);
        }

        // Rationale:
        // 1. Start with the larger of (the target size or the initial capacity)
        // 2. Scale by the inverse of the load factor
        // 3. Scale by 2 (you might have gotten unlucky and gotten just to the threshold and then doubled)
        // 4. Fudge by scaling by 2 (you might have gotten unlucky and had just enough deleted items sitting in slots
        final int expectedCapacityLimit = 2 * (int) (Math.max(size, initialCapacity) / loadFactor);
        final int fudgedLimit = expectedCapacityLimit * 2;
        final int actualCapacity = map.capacity();
        if (actualCapacity > fudgedLimit) {
            String message = String.format("actualCapacity (%d) <= fudgedLimit (%d)", actualCapacity, fudgedLimit);
            assertTrue(message, actualCapacity <= fudgedLimit);
        }
    }

    @Test
    public void resetToNullRetainingCapacityRemembersCapacity() {
        // The reference fastutil implementation doesn't have resetToNullRetainingCapacity
        if (factory == referenceFactory) {
            return;
        }
        final int size = 1000;
        final NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.defaultReturnValue();

        // Resetting a never-allocated map is a no-op.
        map.resetToNullRetainingCapacity();
        assertEquals(0, map.capacity());
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        assertEquals(noEntryValue, scalarAccess.get(0));

        for (int ii = 0; ii < size; ++ii) {
            scalarAccess.put(ii * 7, ii);
        }
        final int filledCapacity = map.capacity();
        map.resetToNullRetainingCapacity();

        // The array is released, so the map holds no storage while it sits empty.
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertEquals(0, map.capacity());
        // resetToNullRetainingCapacity() is not a cursor operation: reset the invalidated binding.
        scalarAccess.reset(map);
        for (int ii = 0; ii < size; ++ii) {
            assertEquals(noEntryValue, scalarAccess.get(ii * 7));
        }

        // The remembered capacity is restored by the next allocation, so refilling to the same size never rehashes.
        scalarAccess.put(0, 1);
        assertEquals(filledCapacity, map.capacity());
        for (int ii = 1; ii < size; ++ii) {
            scalarAccess.put(ii * 7, ii + 1);
        }
        assertEquals(filledCapacity, map.capacity());
        // The cursor did all the refilling itself, so its binding is still fresh for the reads.
        for (int ii = 1; ii < size; ++ii) {
            assertEquals(ii + 1, scalarAccess.get(ii * 7));
        }
    }

    @Test
    public void iteratorFromEmptyAndNullMap() {
        NullableLongLongMap map = factory.create(initialCapacity, loadFactor);
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(map);
        scalarAccess.put(0, 1);
        scalarAccess.put(2, 3);
        map.clear();
        emptyMapHelper(map);
        if (factory == referenceFactory) {
            return;
        }
        map.resetToNull();
        emptyMapHelper(map);
    }

    private void emptyMapHelper(NullableLongLongMap map) {
        final MutableInt count = new MutableInt();
        map.forEach((key, value) -> count.increment());
        assertEquals(0, count.get());
    }

    static class Factory {
        private final String name;
        private final int entriesPerBucket;
        private final BiFunction<Integer, Float, NullableLongLongMap> constructor;

        Factory(String name, int entriesPerBucket, BiFunction<Integer, Float, NullableLongLongMap> constructor) {
            this.name = name;
            this.entriesPerBucket = entriesPerBucket;
            this.constructor = constructor;
        }

        @Override
        public String toString() {
            return name;
        }

        public int getEntriesPerBucket() {
            return entriesPerBucket;
        }

        public NullableLongLongMap create(int initialCapacity, float loadFactor) {
            return constructor.apply(initialCapacity, loadFactor);
        }
    }

    static class Entries {
        public static Entries create(Map<Long, Long> map) {
            int size = map.size();
            final long[] keys = new long[size];
            final long[] values = new long[size];
            int nextIndex = 0;
            for (Map.Entry<Long, Long> entry : map.entrySet()) {
                keys[nextIndex] = entry.getKey();
                values[nextIndex] = entry.getValue();
                ++nextIndex;
            }
            assertEquals(nextIndex, size);
            return new Entries(keys, values);
        }

        public static Entries create(NullableLongLongMap map) {
            int size = map.size();
            final long[] keys = new long[size];
            final long[] values = new long[size];
            final MutableInt nextIndex = new MutableInt();
            map.forEach((key, value) -> {
                keys[nextIndex.get()] = key;
                values[nextIndex.getAndIncrement()] = value;
            });
            assertEquals(size, nextIndex.get());
            return new Entries(keys, values);
        }

        private final long[] keys;
        private final long[] values;

        Entries(long[] keys, long[] values) {
            assertEquals(keys.length, values.length);
            this.keys = keys;
            this.values = values;
        }

        boolean destructivelyEquals(Entries other) {
            Arrays.sort(keys);
            Arrays.sort(values);
            Arrays.sort(other.keys);
            Arrays.sort(other.values);
            final boolean keysEqual = Arrays.equals(keys, other.keys);
            final boolean valuesEqual = Arrays.equals(values, other.values);
            return keysEqual && valuesEqual;
        }
    }

    private static void populate(Random rng, int numIterations, long randomRange, double putProbability,
            Map<Long, Long> reference, NullableLongLongMap test) {
        final NullableLongLongMap.ScalarAccess scalarAccess = new NullableLongLongMap.ScalarAccess(test);
        for (int ii = 0; ii < numIterations; ++ii) {
            final long nextKey = Math.abs(rng.nextLong()) % randomRange;
            final long nextValue = ii;

            if (rng.nextDouble() < putProbability) {
                reference.put(nextKey, nextValue);
                scalarAccess.put(nextKey, nextValue);
            } else {
                reference.remove(nextKey);
                scalarAccess.remove(nextKey);
            }
        }
    }

    private static class TestNullableLongLongMap implements NullableLongLongMapTestAccessors {
        final Long2LongOpenHashMap map;

        public TestNullableLongLongMap(int initialCapacity, float loadFactor) {
            map = new Long2LongOpenHashMap(initialCapacity, loadFactor);
            map.defaultReturnValue(-1);
        }

        @Override
        public void resetToNull() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resetToNullRetainingCapacity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int capacity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long[] keyArray() {
            return map.keySet().toLongArray();
        }

        @Override
        public long[] keyArray(long[] space) {
            return map.keySet().toArray(space);
        }

        @Override
        public long[] valueArray() {
            return map.values().toLongArray();
        }

        @Override
        public long[] valueArray(long[] space) {
            return map.values().toArray(space);
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public long defaultReturnValue() {
            return map.defaultReturnValue();
        }

        @Override
        public void put(LongChunk<? extends Any> keys, LongChunk<? extends Any> values,
                WritableLongChunk<? extends Any> oldValues) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                oldValues.set(ii, map.put(keys.get(ii), values.get(ii)));
            }
            oldValues.setSize(size);
        }

        @Override
        public void putIfAbsent(LongChunk<? extends Any> keys, LongChunk<? extends Any> values,
                WritableLongChunk<? extends Any> oldValues) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                oldValues.set(ii, map.putIfAbsent(keys.get(ii), values.get(ii)));
            }
            oldValues.setSize(size);
        }

        @Override
        public void put(LongChunk<? extends Any> keys, LongChunk<? extends Any> values) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                map.put(keys.get(ii), values.get(ii));
            }
        }

        @Override
        public void put(LongChunk<? extends Any> keys, long value) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                map.put(keys.get(ii), value);
            }
        }

        @Override
        public void get(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> result) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                result.set(ii, map.get(keys.get(ii)));
            }
            result.setSize(size);
        }

        @Override
        public void remove(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> oldValues) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                oldValues.set(ii, map.remove(keys.get(ii)));
            }
            oldValues.setSize(size);
        }

        @Override
        public void clear() {
            map.clear();
        }

        @Override
        public void forEach(LongLongBiConsumer consumer) {
            map.forEach(consumer);
        }
    }
}
