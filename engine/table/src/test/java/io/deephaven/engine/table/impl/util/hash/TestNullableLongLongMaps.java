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
        // Leave some tombstones behind, so the drain has to walk past them.
        for (long key = 0; key < 10_000; key += 3) {
            cursor.remove(key);
            reference.remove(key);
        }

        final NullableLongLongMap upgraded = NullableLongLongMaps.maybeUpgrade(map, DENSE, 1);
        assertTrue(upgraded instanceof HashMapLockFreeK4V4WithAMAC);
        assertEquals(NO_ENTRY_VALUE, upgraded.defaultReturnValue());
        assertEquals(reference.size(), upgraded.size());
        cursor.reset(upgraded);
        for (long key = 0; key < 10_000; ++key) {
            final long expected = reference.getOrDefault(key, NO_ENTRY_VALUE);
            assertEquals(expected, cursor.get(key));
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
    public void alreadyWindowedReturnsTheSameMap() {
        final NullableLongLongMap map = HashMapLockFreeK4V4WithAMAC.of(16, DENSE, NO_ENTRY_VALUE);
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < 100; ++key) {
            cursor.put(key, key);
        }
        assertSame(map, NullableLongLongMaps.maybeUpgrade(map, DENSE, 1));
    }

    @Test
    public void sparseLoadFactorReturnsTheSameMap() {
        final NullableLongLongMap map = HashMapLockFreeK1V1.of(16, SPARSE, NO_ENTRY_VALUE);
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < 100; ++key) {
            cursor.put(key, key);
        }
        // Big enough (threshold 1) but not dense enough: no upgrade.
        assertSame(map, NullableLongLongMaps.maybeUpgrade(map, SPARSE, 1));
    }

    @Test
    public void ceilingTriggerOverridesSparseLoadFactor() {
        final NullableLongLongMap map = HashMapLockFreeK1V1.of(16, SPARSE, NO_ENTRY_VALUE);
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(map);
        for (long key = 0; key < 100; ++key) {
            cursor.put(key, key + 1);
        }
        // Sparse and below the size threshold, but "within reach of the ceiling" (tiny ceiling for the test):
        // forced-dense wins the argument.
        final NullableLongLongMap upgraded = NullableLongLongMaps.maybeUpgrade(map, SPARSE, 1000, 50);
        assertTrue(upgraded instanceof HashMapLockFreeK4V4WithAMAC);
        assertEquals(100, upgraded.size());
        cursor.reset(upgraded);
        for (long key = 0; key < 100; ++key) {
            assertEquals(key + 1, cursor.get(key));
        }
    }

    @Test
    public void ofExpectedSizeChoosesShapeBySizeAndDensity() {
        assertTrue(NullableLongLongMaps.ofExpectedSize(10, DENSE, NO_ENTRY_VALUE,
                1000) instanceof HashMapLockFreeK4V4);
        assertTrue(NullableLongLongMaps.ofExpectedSize(1000, DENSE, NO_ENTRY_VALUE,
                1000) instanceof HashMapLockFreeK4V4WithAMAC);
        // Big enough but not dense enough: serial.
        assertTrue(NullableLongLongMaps.ofExpectedSize(1000, SPARSE, NO_ENTRY_VALUE,
                1000) instanceof HashMapLockFreeK4V4);
    }
}
