//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.util.hash.NullableLongLongMaps.ReadMode;
import io.deephaven.engine.table.impl.util.hash.NullableLongLongMaps.Shape;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongUnaryOperator;
import static org.junit.Assert.*;

public class TestNullableLongLongMaps {
    // On either side of NullableLongLongMaps.AMAC_LOAD_FACTOR_FLOOR.
    private static final double DENSE = 0.9;
    private static final double SPARSE = 0.5;
    private static final long NO_ENTRY_VALUE = -7;

    @Test
    public void shapeForRebuildNeverNarrowsAndWidensWhenDenseOrAtTheCeiling() {
        final int big = NullableLongLongMaps.DEFAULT_AMAC_THRESHOLD_ENTRIES;
        for (final Shape narrow : new Shape[] {Shape.K1V1, Shape.K2V2}) {
            // Dense and big: widen.
            assertEquals(Shape.K4V4, NullableLongLongMaps.shapeForRebuild(narrow, DENSE, big, false));
            // Dense but not yet big, or big but sparse: stay.
            assertEquals(narrow, NullableLongLongMaps.shapeForRebuild(narrow, DENSE, big - 1, false));
            assertEquals(narrow, NullableLongLongMaps.shapeForRebuild(narrow, SPARSE, big, false));
            assertEquals(narrow, NullableLongLongMaps.shapeForRebuild(narrow, SPARSE, 16, false));
            // At the ceiling: widen whatever the load factor says.
            assertEquals(Shape.K4V4, NullableLongLongMaps.shapeForRebuild(narrow, SPARSE, 16, true));
        }
        // Never narrower.
        assertEquals(Shape.K4V4, NullableLongLongMaps.shapeForRebuild(Shape.K4V4, SPARSE, 16, false));
        assertEquals(Shape.K4V4, NullableLongLongMaps.shapeForRebuild(Shape.K4V4, DENSE, big, true));
    }

    private static long valueFor(final long key) {
        return key * 3 + 1;
    }

    /**
     * A narrow map at a dense load factor widens itself at some rehash on the way past the threshold, keeping every
     * mapping and every tombstone's effect, and keeps working as the wide map it has become.
     */
    @Test
    public void denseMapsWidenAsTheyGrowAndKeepEverything() {
        final int n = 1_200_000;
        for (final Shape born : new Shape[] {Shape.K1V1, Shape.K2V2}) {
            final NullableLongLongMap map = NullableLongLongMaps.of(born, 16, DENSE, NO_ENTRY_VALUE);
            final HashMapLockFreeKnVn knVn = (HashMapLockFreeKnVn) map;
            assertEquals(born, knVn.shape());
            final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
            int expectedSize = 0;
            for (long key = 0; key < n; ++key) {
                cursor.put(key, valueFor(key));
                ++expectedSize;
                if (key % 7 == 3) {
                    // Tombstones along the way, on both sides of the transition.
                    cursor.remove(key - 3);
                    --expectedSize;
                }
            }
            assertEquals(born.name(), Shape.K4V4, knVn.shape());
            assertEquals(born.name(), expectedSize, map.size());
            assertEquals(born.name(), 4, HashMapLockFreeKnVn.shapeTagOf(knVn.keysAndValuesSnapshot()));
            checkContents(map, n, key -> key % 7 == 0 ? NO_ENTRY_VALUE : valueFor(key));
            // Still a working map afterwards: more puts, more removes.
            for (long key = n; key < n + 10_000; ++key) {
                cursor.put(key, valueFor(key));
            }
            for (long key = 0; key < 10_000; ++key) {
                cursor.remove(key);
            }
            assertEquals(born.name(), expectedSize + 10_000 - (10_000 - 10_000 / 7 - 1), map.size());
        }
    }

    @Test
    public void sparseMapsStayNarrowAndWideMapsStayWide() {
        final int n = 1_200_000;
        for (final Shape born : Shape.values()) {
            final NullableLongLongMap map = NullableLongLongMaps.of(born, 16, SPARSE, NO_ENTRY_VALUE);
            final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
            for (long key = 0; key < n; ++key) {
                cursor.put(key, valueFor(key));
            }
            assertEquals(born.name(), born, ((HashMapLockFreeKnVn) map).shape());
            assertEquals(born.name(), n, map.size());
            checkContents(map, n, TestNullableLongLongMaps::valueFor);
        }
    }

    @Test
    public void presizedDenseMapsAreBuiltWide() {
        final int big = NullableLongLongMaps.DEFAULT_AMAC_THRESHOLD_ENTRIES;
        for (final Shape born : new Shape[] {Shape.K1V1, Shape.K2V2}) {
            final HashMapLockFreeKnVn wide =
                    (HashMapLockFreeKnVn) NullableLongLongMaps.of(born, 2 * big, DENSE, NO_ENTRY_VALUE);
            assertEquals(Shape.K4V4, wide.shape());
            final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(wide);
            cursor.put(1, 2);
            assertEquals(Shape.K4V4, wide.shape());
            assertEquals(4, HashMapLockFreeKnVn.shapeTagOf(wide.keysAndValuesSnapshot()));
            // Small, or sparse: as requested.
            assertEquals(born,
                    ((HashMapLockFreeKnVn) NullableLongLongMaps.of(born, 1000, DENSE, NO_ENTRY_VALUE)).shape());
            assertEquals(born,
                    ((HashMapLockFreeKnVn) NullableLongLongMaps.of(born, 2 * big, SPARSE, NO_ENTRY_VALUE)).shape());
        }
    }

    /**
     * Widening is a one-way door across resets too, and across each kind of reset on its own: a map that has widened
     * comes back wide from a capacity-retaining reset (the same entries must refill into the same array) and from a
     * plain reset, which forgets the capacity but not the shape. Each reset gets its own freshly widened map, so that
     * neither can do the other's remembering.
     */
    @Test
    public void widenedMapsComeBackWideFromResets() {
        final NullableLongLongMap retaining = newWidenedMap();
        final HashMapLockFreeKnVn retainingKnVn = (HashMapLockFreeKnVn) retaining;
        final int capacityWhenWide = retaining.capacity();
        retaining.resetToNullRetainingCapacity();
        assertEquals(Shape.K4V4, retainingKnVn.shape());
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(retaining);
        cursor.put(1, 2);
        assertEquals(Shape.K4V4, retainingKnVn.shape());
        assertEquals(capacityWhenWide, retaining.capacity());
        assertEquals(2, cursor.get(1));

        final NullableLongLongMap plain = newWidenedMap();
        final HashMapLockFreeKnVn plainKnVn = (HashMapLockFreeKnVn) plain;
        plain.resetToNull();
        assertEquals(Shape.K4V4, plainKnVn.shape());
        cursor.reset(plain);
        cursor.put(1, 2);
        assertEquals(Shape.K4V4, plainKnVn.shape());
        assertTrue(plain.capacity() < capacityWhenWide);
        assertEquals(2, cursor.get(1));
    }

    private static NullableLongLongMap newWidenedMap() {
        final NullableLongLongMap map = NullableLongLongMaps.of(Shape.K1V1, 16, DENSE, NO_ENTRY_VALUE);
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < 1_200_000; ++key) {
            cursor.put(key, valueFor(key));
        }
        assertEquals(Shape.K4V4, ((HashMapLockFreeKnVn) map).shape());
        return map;
    }

    /**
     * A reader taking chunked snapshots while a single writer grows a narrow map through the widening rehash: every key
     * the writer published before a read began is present and correct in the snapshot that read took, whichever shape
     * that snapshot has. (Keys beyond the published mark are the writer's business, and the reader ignores them.)
     */
    @Test
    public void readersSeeCompleteSnapshotsThroughWidening() throws InterruptedException {
        final int n = 1_500_000;
        final NullableLongLongMap map = NullableLongLongMaps.of(Shape.K1V1, 16, DENSE, NO_ENTRY_VALUE);
        final HashMapLockFreeKnVn knVn = (HashMapLockFreeKnVn) map;
        // Keys [0, written) are fully inserted; the volatile write publishes them, the volatile read acquires them.
        final AtomicInteger written = new AtomicInteger();
        final Thread writer = new Thread(() -> {
            final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
            for (int key = 0; key < n; ++key) {
                cursor.put(key, valueFor(key));
                if ((key & 1023) == 1023) {
                    written.set(key + 1);
                }
            }
            written.set(n);
        }, "writer");
        writer.start();
        final int chunkSize = 4096;
        final long[] keys = new long[chunkSize];
        final WritableLongChunk<Any> values = WritableLongChunk.writableChunkWrap(new long[chunkSize]);
        final Random rng = new Random(20260926);
        do {
            final int limit = written.get();
            if (limit == 0) {
                continue;
            }
            final int begin = rng.nextInt(Math.max(1, limit - chunkSize + 1));
            final int count = Math.min(chunkSize, limit - begin);
            for (int ii = 0; ii < count; ++ii) {
                keys[ii] = begin + ii;
            }
            map.get(LongChunk.chunkWrap(keys, 0, count), values);
            assertEquals(count, values.size());
            for (int ii = 0; ii < count; ++ii) {
                assertEquals("key " + keys[ii], valueFor(keys[ii]), values.get(ii));
            }
        } while (writer.isAlive());
        writer.join();
        assertEquals(Shape.K4V4, knVn.shape());
        assertEquals(n, map.size());
        checkContents(map, n, TestNullableLongLongMaps::valueFor);
    }

    private static void checkContents(final NullableLongLongMap map, final int n, final LongUnaryOperator expected) {
        final int chunkSize = 4096;
        final long[] keys = new long[chunkSize];
        final WritableLongChunk<Any> values = WritableLongChunk.writableChunkWrap(new long[chunkSize]);
        for (int begin = 0; begin < n; begin += chunkSize) {
            final int count = Math.min(chunkSize, n - begin);
            for (int ii = 0; ii < count; ++ii) {
                keys[ii] = begin + ii;
            }
            map.get(LongChunk.chunkWrap(keys, 0, count), values);
            for (int ii = 0; ii < count; ++ii) {
                assertEquals("key " + keys[ii], expected.applyAsLong(keys[ii]), values.get(ii));
            }
        }
    }

    @Test
    public void wantSerialForMonotoneKeysIsAnOccupancyThreshold() {
        final int capacity = 1_000_000;
        final int threshold = (int) (capacity * NullableLongLongMaps.MONOTONE_KEYS_SERIAL_BELOW_OCCUPANCY);
        // Sparse: a monotone local chunk takes the serial loop, right up to the threshold.
        assertTrue(NullableLongLongMaps.wantSerialForMonotoneKeys(0, capacity));
        assertTrue(NullableLongLongMaps.wantSerialForMonotoneKeys(threshold - 1, capacity));
        // At and above it: the window, whatever the key order.
        assertFalse(NullableLongLongMaps.wantSerialForMonotoneKeys(threshold, capacity));
        assertFalse(NullableLongLongMaps.wantSerialForMonotoneKeys(capacity, capacity));
        // The threshold is where the data put it: a load-factor-0.5 map never reaches it, a dense map lives above it.
        assertEquals(0.5, NullableLongLongMaps.MONOTONE_KEYS_SERIAL_BELOW_OCCUPANCY, 0.0);
    }

    /**
     * A batch of puts that is a map's first write allocates for the batch: the map lands at the capacity a map presized
     * for that many entries would have, rather than rehashing its way up through the batch. The remembered capacity
     * wins when it is larger, so a small first batch after a capacity-retaining reset still comes back at full size.
     */
    @Test
    public void firstBatchOfPutsAllocatesForItsSize() {
        final int n = 100_000;
        final long[] keys = new long[n];
        final long[] values = new long[n];
        for (int ii = 0; ii < n; ++ii) {
            keys[ii] = 3L * ii + 1;
            values[ii] = valueFor(keys[ii]);
        }
        for (final Shape shape : Shape.values()) {
            final NullableLongLongMap presized = NullableLongLongMaps.ofExpectedSize(shape, n, DENSE, NO_ENTRY_VALUE);
            new NullableLongLongMap.ScalarAccess(presized).put(keys[0], values[0]);
            final int presizedCapacity = presized.capacity();

            final NullableLongLongMap map = NullableLongLongMaps.of(shape, 16, DENSE, NO_ENTRY_VALUE);
            map.put(LongChunk.chunkWrap(keys), LongChunk.chunkWrap(values));
            assertEquals(shape.name(), presizedCapacity, map.capacity());
            assertEquals(shape.name(), n, map.size());
            checkContents(map, 3 * n, key -> key % 3 == 1 ? valueFor(key) : NO_ENTRY_VALUE);

            // The remembered capacity is the floor: a three-key batch after a capacity-retaining reset lands back at
            // full size, while the same batch after a plain reset gets a small array.
            map.resetToNullRetainingCapacity();
            map.put(LongChunk.chunkWrap(keys, 0, 3), LongChunk.chunkWrap(values, 0, 3));
            assertEquals(shape.name(), presizedCapacity, map.capacity());
            final NullableLongLongMap fresh = NullableLongLongMaps.of(shape, 16, DENSE, NO_ENTRY_VALUE);
            fresh.put(LongChunk.chunkWrap(keys, 0, 3), LongChunk.chunkWrap(values, 0, 3));
            assertTrue(shape.name(), fresh.capacity() < presizedCapacity);
            assertEquals(shape.name(), 3, fresh.size());
        }
    }

    @Test
    public void isLocalWalkIsAnAverageStepThreshold() {
        final int step = NullableLongLongMaps.MONOTONE_KEYS_MAX_LOCAL_STEP;
        // Consecutive keys, and a step exactly at the threshold, are local; one beyond is not; direction is irrelevant.
        assertTrue(NullableLongLongMaps.isLocalWalk(1000, 1000 + 4095, 4096));
        assertTrue(NullableLongLongMaps.isLocalWalk(0, (long) step * 4095, 4096));
        assertFalse(NullableLongLongMaps.isLocalWalk(0, (long) (step + 1) * 4095, 4096));
        assertTrue(NullableLongLongMaps.isLocalWalk(-1000, -1000 - 4095, 4096));
        assertFalse(NullableLongLongMaps.isLocalWalk(0, -((long) (step + 1) * 4095), 4096));
        // Degenerate chunks are local; a span that overflows a long is not.
        assertTrue(NullableLongLongMaps.isLocalWalk(7, 7, 1));
        assertTrue(NullableLongLongMaps.isLocalWalk(7, 9, 2));
        assertFalse(NullableLongLongMaps.isLocalWalk(Long.MIN_VALUE + 1, Long.MAX_VALUE - 1, 4096));
    }

    @Test
    public void wantWindowedReadsGatesOnFootprintAndChunkSize() {
        final int threshold = NullableLongLongMaps.DEFAULT_AMAC_THRESHOLD_ENTRIES;
        final int minChunk = NullableLongLongMaps.MIN_WINDOWED_CHUNK;
        // At and above the footprint threshold, with a chunk that fills the window: windowed, however full the map
        // happens to be (occupancy is not an input — it sawtooths with rehash and turned out to be second-order; see
        // the javadoc).
        assertTrue(NullableLongLongMaps.wantWindowedReads(threshold, minChunk));
        assertTrue(NullableLongLongMaps.wantWindowedReads(Integer.MAX_VALUE, minChunk));
        assertTrue(NullableLongLongMaps.wantWindowedReads(threshold, 4096));
        // Below the footprint threshold (cache-resident): serial, whatever the chunk.
        assertFalse(NullableLongLongMaps.wantWindowedReads(threshold - 1, minChunk));
        assertFalse(NullableLongLongMaps.wantWindowedReads(threshold - 1, 4096));
        assertFalse(NullableLongLongMaps.wantWindowedReads(0, 4096));
        // A chunk too narrow to fill the window: serial, however large the map — the scalar cursor's single-key
        // chunk above all.
        assertFalse(NullableLongLongMaps.wantWindowedReads(threshold, minChunk - 1));
        assertFalse(NullableLongLongMaps.wantWindowedReads(Integer.MAX_VALUE, 1));
        assertFalse(NullableLongLongMaps.wantWindowedReads(Integer.MAX_VALUE, 0));
    }

    @Test
    public void factoryBuildsTheRequestedShape() {
        for (final Shape shape : Shape.values()) {
            final NullableLongLongMap map = NullableLongLongMaps.of(shape, 16, DENSE, NO_ENTRY_VALUE);
            final HashMapLockFreeKnVn knVn = (HashMapLockFreeKnVn) map;
            // Before the first write the requested shape is a promise the map remembers; after it, the array's own
            // tag keeps it.
            assertEquals(shape, knVn.shape());
            assertEquals(NO_ENTRY_VALUE, map.defaultReturnValue());
            final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
            cursor.put(1, 2);
            assertEquals(shape, knVn.shape());
            assertEquals(shape.bucketWidth(), HashMapLockFreeKnVn.shapeTagOf(knVn.keysAndValuesSnapshot()));
            assertEquals(shape, Shape.forBucketWidth(shape.bucketWidth()));
        }
    }

    @Test
    public void windowModeRequiresK4V4() {
        for (final Shape shape : new Shape[] {Shape.K1V1, Shape.K2V2}) {
            // The narrow shapes have no window kernel.
            final IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> NullableLongLongMaps.of(shape, 16, DENSE, NO_ENTRY_VALUE, ReadMode.WINDOW));
            assertTrue(iae.getMessage(), iae.getMessage().contains(shape.name()));
            // SERIAL is truthful for every shape.
            assertNotNull(NullableLongLongMaps.of(shape, 16, DENSE, NO_ENTRY_VALUE, ReadMode.SERIAL));
        }
        assertNotNull(NullableLongLongMaps.of(Shape.K4V4, 16, DENSE, NO_ENTRY_VALUE, ReadMode.WINDOW));
    }

    @Test
    public void forBucketWidthRejectsUnsupportedWidths() {
        for (final int width : new int[] {0, 3, 8, -1}) {
            // Only 1, 2 and 4 are shapes.
            final IllegalArgumentException iae =
                    assertThrows(IllegalArgumentException.class, () -> Shape.forBucketWidth(width));
            assertTrue(iae.getMessage(), iae.getMessage().contains(Integer.toString(width)));
        }
    }
}
