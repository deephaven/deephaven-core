//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;

public class TestImmutableConcurrentCache extends BaseArrayTestCase {

    private static class Value extends ImmutableConcurrentCache.Pair<String> {
        Value(int key, String value) {
            super(key, value);
        }
    }

    private static Value makeVal(Integer key) {
        final Character c = (char) (key + 'a');

        if (c.equals('d')) {
            return null;
        }

        return new Value(key, c.toString().toUpperCase());
    }

    public void testCache() {
        final ImmutableConcurrentCache<Value> cache =
                new ImmutableConcurrentCache<>(10, TestImmutableConcurrentCache::makeVal);

        assertEquals("A", cache.computeIfAbsent(0).getValue());
        assertEquals("A", cache.computeIfAbsent(0).getValue());

        assertEquals("A", cache.computeIfAbsent(0).getValue());
        assertEquals("B", cache.computeIfAbsent(1).getValue());
        assertEquals("C", cache.computeIfAbsent(2).getValue());

        try {
            cache.computeIfAbsent(3);
            fail("Expected exception");
        } catch (final IllegalArgumentException e) {
            // pass
        }

        cache.clear();
        assertEquals("A", cache.computeIfAbsent(0).getValue());
        assertEquals("B", cache.computeIfAbsent(1).getValue());
        assertEquals("C", cache.computeIfAbsent(2).getValue());

        try {
            cache.computeIfAbsent(3);
            fail("Expected exception");
        } catch (final IllegalArgumentException e) {
            // pass
        }
    }
}
