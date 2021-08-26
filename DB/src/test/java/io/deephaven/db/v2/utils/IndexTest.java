package io.deephaven.db.v2.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class IndexTest {
    private static class FakeIterator implements Index.Iterator {
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
        final Index.Iterator it = new FakeIterator();
        assertFalse(it.forEachLong((v) -> false));
    }

    @Test
    public void testPosReturnValueIteratorForEach() {
        final Index.Iterator it = new FakeIterator();
        assertTrue(it.forEachLong((v) -> true));
    }
}
