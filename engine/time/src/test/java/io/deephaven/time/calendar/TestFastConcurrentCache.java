package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.util.List;

public class TestFastConcurrentCache extends BaseArrayTestCase {

    public void testCache() {
        final FastConcurrentCache<String, String> cache = new FastConcurrentCache<>(String::toUpperCase);

        assertFalse(cache.isFastCache());
        assertEquals("A", cache.get("a"));
        assertEquals("A", cache.get("a"));

        cache.enableFastCache(List.of("a", "b", "c"), true);

        assertTrue(cache.isFastCache());
        assertEquals("A", cache.get("a"));
        assertEquals("B", cache.get("b"));
        assertEquals("C", cache.get("c"));

        try{
            cache.get("d");
            fail("Expected exception");
        } catch (final RuntimeException e) {
            //pass
        }

        try{
            // cache is already enabled
            cache.enableFastCache(List.of("a", "b", "c"), true);
            fail("Expected exception");
        } catch (final IllegalStateException e) {
            //pass
        }

        cache.clear();
        assertFalse(cache.isFastCache());
        assertEquals("A", cache.get("a"));
        assertEquals("B", cache.get("b"));
        assertEquals("C", cache.get("c"));
        assertEquals("D", cache.get("d"));

        cache.clear();
        assertFalse(cache.isFastCache());
        assertEquals("A", cache.get("a"));
        assertEquals("B", cache.get("b"));
        assertEquals("C", cache.get("c"));
        assertEquals("D", cache.get("d"));
    }
}
