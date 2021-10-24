package io.deephaven.engine.v2.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class TrackingMutableRowSetTest {
    private static class FakeIterator implements TrackingMutableRowSet.Iterator {
        long curr = 10;

        @Override
        public void close() {}

        @Override
        public long nextLong() {
            return --curr;
        }

        @Override
        public boolean hasNext() {
            return curr > 0;
        }
    }

    @Test
    public void testNegReturnValueIteratorForEach() {
        final TrackingMutableRowSet.Iterator it = new FakeIterator();
        assertFalse(it.forEachLong((v) -> false));
    }

    @Test
    public void testPosReturnValueIteratorForEach() {
        final TrackingMutableRowSet.Iterator it = new FakeIterator();
        assertTrue(it.forEachLong((v) -> true));
    }
}
