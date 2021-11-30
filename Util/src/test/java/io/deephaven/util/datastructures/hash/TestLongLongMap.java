/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.datastructures.hash;

import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import io.deephaven.util.datastructures.hash.*;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.function.BiFunction;

@RunWith(Parameterized.class)
public class TestLongLongMap {
    private static final Factory troveFactory = new Factory("Trove", TLongLongHashMap::new);

    @Parameterized.Parameters(name = "map={0}, cap={1}, load={2}")
    public static Iterable<Object[]> data() {
        List<Object[]> result = new ArrayList<>();
        final Factory[] factories = {
                troveFactory,
                new Factory("K1V1", HashMapLockFreeK1V1::new),
                new Factory("K2V2", HashMapLockFreeK2V2::new),
                new Factory("K4V4", HashMapLockFreeK4V4::new)
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
        TLongLongMap map = factory.create(initialCapacity, loadFactor);
        map.put(0, 12345);
        TestCase.assertEquals(map.get(0), 12345);
        TestCase.assertEquals(map.size(), 1);
    }

    @Test
    public void badKeys() {
        // Trove doesn't have key limitations
        if (factory == troveFactory) {
            return;
        }
        TLongLongMap map = factory.create(initialCapacity, loadFactor);
        try {
            map.put(HashMapBase.SPECIAL_KEY_FOR_DELETED_SLOT, 12345);
            TestCase.fail("SPECIAL_KEY_FOR_DELETED_SLOT should not be accepted");
        } catch (io.deephaven.base.verify.AssertionFailure e) {
            // do nothing
        }
        try {
            map.put(HashMapBase.REDIRECTED_KEY_FOR_EMPTY_SLOT, 12345);
            TestCase.fail("REDIRECTED_KEY_FOR_EMPTY_SLOT should not be accepted");
        } catch (io.deephaven.base.verify.AssertionFailure e) {
            // do nothing
        }
    }

    @Test
    public void nullMapReturnsNoEntry() {
        // Trove doesn't have setToNull
        if (factory == troveFactory) {
            return;
        }
        TNullableLongLongMap map = (TNullableLongLongMap) factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.getNoEntryValue();
        map.put(0, 1);
        map.put(2, 3);
        map.resetToNull();
        for (int ii = 0; ii < 4; ++ii) {
            TestCase.assertEquals(map.get(ii), noEntryValue);
        }
    }

    @Test
    public void prevValuesOnInsert() {
        final long beginKey = 100;
        final long endKey = 200;
        final long beginValue = 5000;
        final long endValue = 5010;
        TLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.getNoEntryValue();
        for (long valueBase = beginValue; valueBase < endValue; ++valueBase) {
            for (long key = beginKey; key < endKey; ++key) {
                final long expectedPrevious = valueBase == beginValue ? noEntryValue : key + valueBase - 1;
                final long actualPrevious = map.put(key, key + valueBase);
                TestCase.assertEquals(expectedPrevious, actualPrevious);
            }
        }
    }

    @Test
    public void prevValuesOnRemove() {
        final long beginKey = 100;
        final long endKey = 200;
        TLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.getNoEntryValue();
        for (long key = beginKey; key < endKey; ++key) {
            final long previous = map.put(key, key - 10000);
            TestCase.assertEquals(previous, noEntryValue);
        }
        for (long key = beginKey; key < endKey; ++key) {
            final long expectedPrevious = key - 10000;
            final long actualPrevious = map.remove(key);
            TestCase.assertEquals(expectedPrevious, actualPrevious);
        }
    }

    @Test
    public void putIfAbsent() {
        final long beginKey = 100;
        final long endKey = 200;
        TLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long noEntryValue = map.getNoEntryValue();
        for (long key = beginKey; key < endKey; key += 2) {
            final long previous = map.put(key, key + 5000);
            TestCase.assertEquals(previous, noEntryValue);
        }
        for (long key = beginKey; key < endKey; ++key) {
            final long expectedPrevious = (key % 2) == 0 ? key + 5000 : noEntryValue;
            final long actualPrevious = map.putIfAbsent(key, key + 10000);
            TestCase.assertEquals(expectedPrevious, actualPrevious);
        }
        for (long key = beginKey; key < endKey; ++key) {
            final long expectedValue = (key % 2) == 0 ? key + 5000 : key + 10000;
            final long actualValue = map.get(key);
            TestCase.assertEquals(expectedValue, actualValue);
        }
    }

    @Test
    public void clear() {
        final int numIterations = 10;
        final int sizeAtWhichToClear = 10000;
        TLongLongMap map = factory.create(initialCapacity, loadFactor);
        for (int iteration = 0; iteration < numIterations; ++iteration) {
            TestCase.assertEquals(map.size(), 0);
            for (long ii = 0; ii < sizeAtWhichToClear; ++ii) {
                map.put(ii, ii + 1);
            }
            TestCase.assertEquals(map.size(), sizeAtWhichToClear);
            map.clear();
        }
    }

    @Test
    public void setToNull() {
        // Trove doesn't have setToNull
        if (factory == troveFactory) {
            return;
        }
        final int numIterations = 10;
        final int sizeAtWhichToClear = 10000;
        TNullableLongLongMap map = (TNullableLongLongMap) factory.create(initialCapacity, loadFactor);
        for (int iteration = 0; iteration < numIterations; ++iteration) {
            TestCase.assertEquals(map.size(), 0);
            for (long ii = 0; ii < sizeAtWhichToClear; ++ii) {
                map.put(ii, ii + 1);
            }
            TestCase.assertEquals(map.size(), sizeAtWhichToClear);
            map.resetToNull();
        }
    }

    @Test
    public void zeroComesBackThroughKeys() {
        TLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long specialKey = HashMapBase.SPECIAL_KEY_FOR_EMPTY_SLOT;
        map.put(specialKey, 12345);
        long[] keys = map.keys();
        TestCase.assertEquals(keys.length, 1);
        TestCase.assertEquals(keys[0], specialKey);
    }

    @Test
    public void testKeysAndValues() {
        Map<Long, Long> reference = new HashMap<>(initialCapacity, loadFactor);
        TLongLongMap test = factory.create(initialCapacity, loadFactor);
        Random rng = new Random(1283712890);
        // populate(rng, 1000000, 10000, 0.75, reference, test);
        populate(rng, 1000000, 10000, 0.75, reference, test);

        final long[] expectedKeys = new long[reference.size()];
        final long[] expectedValues = new long[reference.size()];
        int nextIndex = 0;
        for (Map.Entry<Long, Long> entry : reference.entrySet()) {
            expectedKeys[nextIndex] = entry.getKey();
            expectedValues[nextIndex] = entry.getValue();
            ++nextIndex;
        }
        TestCase.assertEquals(nextIndex, reference.size());
        TestCase.assertEquals(reference.size(), test.size());

        final long[] actualKeys = test.keys();
        final long[] actualValues = test.values();

        TestCase.assertEquals(expectedKeys.length, actualKeys.length);
        TestCase.assertEquals(expectedValues.length, actualValues.length);

        Arrays.sort(expectedKeys);
        Arrays.sort(expectedValues);
        Arrays.sort(actualKeys);
        Arrays.sort(actualValues);

        TestCase.assertTrue(Arrays.equals(expectedKeys, actualKeys));
        TestCase.assertTrue(Arrays.equals(expectedValues, actualValues));

        final long[] myKeySpace = new long[reference.size()];
        final long[] myValueSpace = new long[reference.size()];

        final long[] keysWithMySpace = test.keys(myKeySpace);
        final long[] valuesWithMySpace = test.values(myValueSpace);
        TestCase.assertSame(keysWithMySpace, myKeySpace);
        TestCase.assertSame(valuesWithMySpace, myValueSpace);
        Arrays.sort(keysWithMySpace);
        Arrays.sort(valuesWithMySpace);
        TestCase.assertTrue(Arrays.equals(expectedKeys, keysWithMySpace));
        TestCase.assertTrue(Arrays.equals(expectedValues, valuesWithMySpace));
    }

    @Test
    public void do100KInserts() {
        TLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long beginKey = -50000;
        final long endKey = 50000;
        final long size = endKey - beginKey;
        final long noEntryValue = map.getNoEntryValue();
        for (long key = beginKey; key < endKey; ++key) {
            map.put(key, key + 1000000);
        }
        TestCase.assertEquals(map.size(), size);
        // These lookups should fail
        for (long key = beginKey - size; key < beginKey; ++key) {
            final long result = map.get(key);
            TestCase.assertEquals(result, noEntryValue);
        }
        // These lookups should succeed
        for (long key = beginKey; key < endKey; ++key) {
            final long result = map.get(key);
            TestCase.assertEquals(result, key + 1000000);
        }
        // These lookups should fail
        for (long key = endKey; key < endKey + size; ++key) {
            final long result = map.get(key);
            TestCase.assertEquals(result, noEntryValue);
        }
    }

    @Test
    public void do100KInsertsThen50KRemoves() {
        TLongLongMap map = factory.create(initialCapacity, loadFactor);
        final long beginKey = 0;
        final long endKey = 100000;
        final long size = endKey - beginKey;
        final long noEntryValue = map.getNoEntryValue();
        for (long key = beginKey; key < endKey; ++key) {
            map.put(key, key + 1000000);
        }
        for (long key = beginKey; key < endKey; key += 2) {
            map.remove(key);
        }
        TestCase.assertEquals(map.size(), size / 2);
        for (long key = beginKey; key < endKey; ++key) {
            final long expectedResult = (key % 2) == 0 ? noEntryValue : key + 1000000;
            final long actualResult = map.get(key);
            TestCase.assertEquals(expectedResult, actualResult);
        }
    }

    @Test
    public void do1MRandomOperationsLotsOfCollisions() {
        // Standard of correctness: java.util.HashMap
        Map<Long, Long> reference = new HashMap<>(initialCapacity, loadFactor);
        TLongLongMap test = factory.create(initialCapacity, loadFactor);
        Random rng = new Random(12345);
        populate(rng, 1000000, 10000, 0.75, reference, test);

        TestCase.assertEquals(reference.size(), test.size());

        Entries masterEntries = Entries.create(reference);
        Entries targetEntries = Entries.create(test);

        TestCase.assertTrue(masterEntries.destructivelyEquals(targetEntries));
    }

    @Test
    public void mapStaysSmall() {
        // no way to ask Trove for capacity
        if (factory == troveFactory) {
            return;
        }
        final int size = 1000;
        final int iterations = 1000000;
        final long randomMod = 1000000000; // 1 billion
        // Use this interface because we want to access 'capacity'
        TNullableLongLongMap map = (TNullableLongLongMap) factory.create(initialCapacity, loadFactor);
        Random insertStream = new Random(67890);
        Random deleteStream = new Random(67890);

        for (int ii = 0; ii < size; ++ii) {
            final long key = insertStream.nextLong() % randomMod;
            final long value = key + 12;
            map.put(key, value);
        }

        for (int ii = 0; ii < iterations; ++ii) {
            final long deleteKey = deleteStream.nextLong() % randomMod;
            map.remove(deleteKey);

            final long key = insertStream.nextLong() % randomMod;
            final long value = key + 12;
            map.put(key, value);
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
            TestCase.assertTrue(message, actualCapacity <= fudgedLimit);
        }
    }

    @Test
    public void iteratorFromEmptyAndNullMap() {
        TLongLongMap map = factory.create(initialCapacity, loadFactor);
        map.put(0, 1);
        map.put(2, 3);
        map.clear();
        emptyMapHelper(map);
        if (factory == troveFactory) {
            return;
        }
        TNullableLongLongMap nullableMap = (TNullableLongLongMap) map;
        nullableMap.resetToNull();
        emptyMapHelper(map);
    }

    private void emptyMapHelper(TLongLongMap map) {
        TLongLongIterator it = map.iterator();
        TestCase.assertFalse(it.hasNext());
        try {
            it.setValue(12345);
            TestCase.fail();
        } catch (Exception e) {
            // do nothing
        }
        try {
            it.advance();
            TestCase.fail();
        } catch (Exception e) {
            // do nothing
        }
        try {
            it.remove();
            TestCase.fail();
        } catch (Exception e) {
            // do nothing
        }
    }

    static class Factory {
        private final String name;
        private BiFunction<Integer, Float, TLongLongMap> constructor;

        Factory(String name, BiFunction<Integer, Float, TLongLongMap> constructor) {
            this.name = name;
            this.constructor = constructor;
        }

        @Override
        public String toString() {
            return name;
        }

        public TLongLongMap create(int initialCapacity, float loadFactor) {
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
            TestCase.assertEquals(nextIndex, size);
            return new Entries(keys, values);
        }

        public static Entries create(TLongLongMap map) {
            int size = map.size();
            final long[] keys = new long[size];
            final long[] values = new long[size];
            int nextIndex = 0;
            for (TLongLongIterator it = map.iterator(); it.hasNext();) {
                it.advance();;
                keys[nextIndex] = it.key();
                values[nextIndex] = it.value();
                ++nextIndex;
            }
            TestCase.assertEquals(nextIndex, size);
            return new Entries(keys, values);
        }

        private final long[] keys;
        private final long[] values;

        Entries(long[] keys, long[] values) {
            TestCase.assertEquals(keys.length, values.length);
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
            Map<Long, Long> reference, TLongLongMap test) {
        for (int ii = 0; ii < numIterations; ++ii) {
            final long nextKey = Math.abs(rng.nextLong()) % randomRange;
            final long nextValue = ii;

            if (rng.nextDouble() < putProbability) {
                reference.put(nextKey, nextValue);
                test.put(nextKey, nextValue);
            } else {
                reference.remove(nextKey);
                test.remove(nextKey);
            }
        }
    }
}
