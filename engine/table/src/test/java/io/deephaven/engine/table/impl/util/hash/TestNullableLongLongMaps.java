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
        assertTrue(upgraded instanceof HashMapLockFreeK4V4);
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
        assertTrue(upgraded instanceof HashMapLockFreeK4V4);
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
        final Map<Shape, Class<?>> expectedClasses = new HashMap<>();
        expectedClasses.put(Shape.K1V1, HashMapLockFreeK1V1.class);
        expectedClasses.put(Shape.K2V2, HashMapLockFreeK2V2.class);
        expectedClasses.put(Shape.K4V4, HashMapLockFreeK4V4.class);
        for (final Shape shape : Shape.values()) {
            final NullableLongLongMap map = NullableLongLongMaps.of(shape, 16, DENSE, NO_ENTRY_VALUE);
            assertEquals(shape.name(), expectedClasses.get(shape), map.getClass());
            assertEquals(NO_ENTRY_VALUE, map.defaultReturnValue());
            assertEquals(shape, Shape.forBucketWidth(shape.bucketWidth()));
        }
    }

    @Test
    public void windowModeRequiresK4V4() {
        for (final Shape shape : new Shape[] {Shape.K1V1, Shape.K2V2}) {
            try {
                NullableLongLongMaps.of(shape, 16, DENSE, NO_ENTRY_VALUE, ReadMode.WINDOW);
                fail("expected IllegalArgumentException for " + shape);
            } catch (final IllegalArgumentException expected) {
                // The narrow shapes have no window kernel.
            }
            // SERIAL is truthful for every shape.
            assertNotNull(NullableLongLongMaps.of(shape, 16, DENSE, NO_ENTRY_VALUE, ReadMode.SERIAL));
        }
        assertNotNull(NullableLongLongMaps.of(Shape.K4V4, 16, DENSE, NO_ENTRY_VALUE, ReadMode.WINDOW));
    }

    @Test
    public void forBucketWidthRejectsUnsupportedWidths() {
        for (final int width : new int[] {0, 3, 8, -1}) {
            try {
                Shape.forBucketWidth(width);
                fail("expected IllegalArgumentException for width " + width);
            } catch (final IllegalArgumentException expected) {
                // Only 1, 2 and 4 are shapes.
            }
        }
    }
}
