//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.util.List;

public class TestImmutableReadHeavyConcurrentCache extends BaseArrayTestCase {

    private static String makeVal(String s) {
        if(s.equals("d")) {
            return null;
        }

        return s.toUpperCase();
    }

    public void testCache() {
        final ImmutableReadHeavyConcurrentCache<String, String> cache = new ImmutableReadHeavyConcurrentCache<>(TestImmutableReadHeavyConcurrentCache::makeVal);

        assertEquals("A", cache.get("a"));
        assertEquals("A", cache.get("a"));

        assertEquals("A", cache.get("a"));
        assertEquals("B", cache.get("b"));
        assertEquals("C", cache.get("c"));

        try {
            cache.get("d");
            fail("Expected exception");
        } catch (final IllegalArgumentException e) {
            // pass
        }

        cache.clear();
        assertEquals("A", cache.get("a"));
        assertEquals("B", cache.get("b"));
        assertEquals("C", cache.get("c"));

        try {
            cache.get("d");
            fail("Expected exception");
        } catch (final IllegalArgumentException e) {
            // pass
        }
    }
}
