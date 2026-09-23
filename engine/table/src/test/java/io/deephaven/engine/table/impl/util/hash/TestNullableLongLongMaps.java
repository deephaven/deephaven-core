//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

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
        final NullableLongLongMap map = HashMapLockFreeK2V2.of(16, DENSE, NO_ENTRY_VALUE);
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
        final NullableLongLongMap map = HashMapLockFreeK1V1.of(16, DENSE, NO_ENTRY_VALUE);
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < 100; ++key) {
            cursor.put(key, key);
        }
        assertSame(map, NullableLongLongMaps.maybeUpgrade(map, DENSE, 1000));
    }

    @Test
    public void sparseLoadFactorReturnsTheSameMap() {
        final NullableLongLongMap map = HashMapLockFreeK1V1.of(16, SPARSE, NO_ENTRY_VALUE);
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < 100; ++key) {
            cursor.put(key, key);
        }
        assertSame(map, NullableLongLongMaps.maybeUpgrade(map, SPARSE, 1));
    }

    @Test
    public void ceilingTriggerOverridesSparseLoadFactor() {
        final NullableLongLongMap map = HashMapLockFreeK1V1.of(16, SPARSE, NO_ENTRY_VALUE);
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
                HashMapLockFreeK4V4.of(16, DENSE, NO_ENTRY_VALUE),
                HashMapLockFreeK4V4WithAMAC.of(16, DENSE, NO_ENTRY_VALUE)}) {
            final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
            for (long key = 0; key < 100; ++key) {
                cursor.put(key, key);
            }
            assertSame(map, NullableLongLongMaps.maybeUpgrade(map, DENSE, 1));
        }
    }

    @Test
    public void wantWindowedReadsGatesOnFootprint() {
        final int threshold = NullableLongLongMaps.DEFAULT_AMAC_THRESHOLD_ENTRIES;
        // At and above the footprint threshold: windowed, however full the map happens to be (occupancy is not an
        // input — it sawtooths with rehash and turned out to be second-order; see the javadoc).
        assertTrue(NullableLongLongMaps.wantWindowedReads(threshold));
        assertTrue(NullableLongLongMaps.wantWindowedReads(Integer.MAX_VALUE));
        // Below it (cache-resident): serial.
        assertFalse(NullableLongLongMaps.wantWindowedReads(threshold - 1));
        assertFalse(NullableLongLongMaps.wantWindowedReads(0));
    }
}
