//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.engine.table.impl.util.hash.NullableLongLongMaps.ReadMode;
import io.deephaven.engine.table.impl.util.hash.NullableLongLongMaps.Shape;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

public class TestNullableLongLongMaps {
    // On either side of NullableLongLongMaps.AMAC_LOAD_FACTOR_FLOOR.
    private static final double DENSE = 0.9;
    private static final double SPARSE = 0.5;
    private static final long NO_ENTRY_VALUE = -7;

    @Test
    public void upgradePreservesEverything() {
        final NullableLongLongMap map = NullableLongLongMaps.of(Shape.K2V2, 16, DENSE, NO_ENTRY_VALUE);
        final Map<Long, Long> reference = new HashMap<>();
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < 10_000; ++key) {
            cursor.put(key, key + 1_000_000);
            reference.put(key, key + 1_000_000);
        }
        for (long key = 0; key < 10_000; key += 3) {
            cursor.remove(key);
            reference.remove(key);
        }
        final NullableLongLongMap upgraded = NullableLongLongMaps.maybeUpgrade(map, DENSE, 1);
        assertEquals(Shape.K4V4, ((HashMapLockFreeKnVn) upgraded).shape());
        assertEquals(NO_ENTRY_VALUE, upgraded.defaultReturnValue());
        assertEquals(reference.size(), upgraded.size());
        cursor.reset(upgraded);
        for (long key = 0; key < 10_000; ++key) {
            assertEquals((long) reference.getOrDefault(key, NO_ENTRY_VALUE), cursor.get(key));
        }
    }

    @Test
    public void belowThresholdReturnsTheSameMap() {
        final NullableLongLongMap map = NullableLongLongMaps.of(Shape.K1V1, 16, DENSE, NO_ENTRY_VALUE);
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < 100; ++key) {
            cursor.put(key, key);
        }
        assertSame(map, NullableLongLongMaps.maybeUpgrade(map, DENSE, 1000));
    }

    @Test
    public void sparseLoadFactorReturnsTheSameMap() {
        final NullableLongLongMap map = NullableLongLongMaps.of(Shape.K1V1, 16, SPARSE, NO_ENTRY_VALUE);
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < 100; ++key) {
            cursor.put(key, key);
        }
        assertSame(map, NullableLongLongMaps.maybeUpgrade(map, SPARSE, 1));
    }

    @Test
    public void ceilingTriggerOverridesSparseLoadFactor() {
        final NullableLongLongMap map = NullableLongLongMaps.of(Shape.K1V1, 16, SPARSE, NO_ENTRY_VALUE);
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < 100; ++key) {
            cursor.put(key, key + 1);
        }
        final NullableLongLongMap upgraded = NullableLongLongMaps.maybeUpgrade(map, SPARSE, 1000, 50);
        assertEquals(Shape.K4V4, ((HashMapLockFreeKnVn) upgraded).shape());
        assertEquals(100, upgraded.size());
        cursor.reset(upgraded);
        for (long key = 0; key < 100; ++key) {
            assertEquals(key + 1, cursor.get(key));
        }
    }

    @Test
    public void alreadyWideReturnsTheSameMap() {
        for (final NullableLongLongMap map : new NullableLongLongMap[] {
                NullableLongLongMaps.of(Shape.K4V4, 16, DENSE, NO_ENTRY_VALUE),
                NullableLongLongMaps.of(Shape.K4V4, 16, DENSE, NO_ENTRY_VALUE, ReadMode.WINDOW)}) {
            final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
            for (long key = 0; key < 100; ++key) {
                cursor.put(key, key);
            }
            assertSame(map, NullableLongLongMaps.maybeUpgrade(map, DENSE, 1));
        }
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
