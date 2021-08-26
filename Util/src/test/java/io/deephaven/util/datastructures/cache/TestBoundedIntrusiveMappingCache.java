package io.deephaven.util.datastructures.cache;

import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

/**
 * Unit test for BoundedIntrusiveMappingCache (focusing on String to int mapping).
 */
public class TestBoundedIntrusiveMappingCache {

    private static class MappingCreationObserver implements ToIntFunction<String> {

        private boolean created = false;

        @Override
        public int applyAsInt(@NotNull final String value) {
            created = true;
            return Integer.parseInt(value);
        }
    }

    @Test
    public void testLRU() {
        final int size = 10;
        final BoundedIntrusiveMappingCache.IntegerImpl<String> cache =
                new BoundedIntrusiveMappingCache.IntegerImpl<>(size);
        final String[] strings = IntStream.range(0, size).mapToObj(Integer::toString).toArray(String[]::new);
        final int addedSize = size / 2;
        final String[] addedStrings =
                IntStream.range(size, size + addedSize).mapToObj(Integer::toString).toArray(String[]::new);

        // Fill the cache initially, to its maximum size
        for (int si = 0; si < strings.length; si++) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(si, cache.computeIfAbsent(strings[si], observer));
            TestCase.assertTrue(observer.created);
        }

        // Make sure it has everything we added
        for (int si = 0; si < strings.length; si++) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(si, cache.computeIfAbsent(strings[si], observer));
            TestCase.assertFalse(observer.created);
        }

        // Prime the set of values we want to be most recently used (odd indexes, of which there are addedSize)
        for (int si = 1; si < strings.length; si += 2) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(si, cache.computeIfAbsent(strings[si], observer));
            TestCase.assertFalse(observer.created);
        }

        // Add more items, displacing the even indexes
        for (int asi = 0; asi < addedStrings.length; asi++) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(size + asi, cache.computeIfAbsent(addedStrings[asi], observer));
            TestCase.assertTrue(observer.created);
        }

        // Re-add the even indexes, displacing the odds, and verify that they were newly created
        for (int si = 0; si < strings.length; si += 2) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(si, cache.computeIfAbsent(strings[si], observer));
            TestCase.assertTrue(observer.created);
        }

        // Re-add the odds, and verify that they were newly created (displacing something)
        for (int si = 1; si < strings.length; si += 2) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(si, cache.computeIfAbsent(strings[si], observer));
            TestCase.assertTrue(observer.created);
        }
    }

    @Test
    public void testFIFO() {
        final int size = 10;
        final BoundedIntrusiveMappingCache.FifoIntegerImpl<String> cache =
                new BoundedIntrusiveMappingCache.FifoIntegerImpl<>(size);
        final String[] strings = IntStream.range(0, size).mapToObj(Integer::toString).toArray(String[]::new);
        final int addedSize = size / 2;
        final String[] addedStrings =
                IntStream.range(size, size + addedSize).mapToObj(Integer::toString).toArray(String[]::new);

        // Fill the cache initially, to its maximum size
        for (int si = 0; si < strings.length; si++) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(si, cache.computeIfAbsent(strings[si], observer));
            TestCase.assertTrue(observer.created);
        }

        // Make sure it has everything we added
        for (int si = 0; si < strings.length; si++) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(si, cache.computeIfAbsent(strings[si], observer));
            TestCase.assertFalse(observer.created);
        }

        // Acccess the odds - not for any particular reason, just to make sure we don't care about access order
        for (int si = 1; si < strings.length; si += 2) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(si, cache.computeIfAbsent(strings[si], observer));
            TestCase.assertFalse(observer.created);
        }

        // Add more items, displacing the earliest added
        for (int asi = 0; asi < addedStrings.length; asi++) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(size + asi, cache.computeIfAbsent(addedStrings[asi], observer));
            TestCase.assertTrue(observer.created);
        }

        // Re-add the items displaced, and verify that they were newly created
        for (int si = 0; si < strings.length - addedStrings.length; si++) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(si, cache.computeIfAbsent(strings[si], observer));
            TestCase.assertTrue(observer.created);
        }

        // Re-add the items displaced by the above re-add, and verify that they were newly created
        for (int si = addedStrings.length; si < strings.length; si++) {
            final MappingCreationObserver observer = new MappingCreationObserver();
            TestCase.assertEquals(si, cache.computeIfAbsent(strings[si], observer));
            TestCase.assertTrue(observer.created);
        }
    }
}
