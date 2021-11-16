package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import org.junit.Test;

import static org.junit.Assert.*;

public class TrackingWritableRowSetTest {
    private static class FakeIterator implements RowSet.Iterator {
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
        final RowSet.Iterator it = new FakeIterator();
        assertFalse(it.forEachLong((v) -> false));
    }

    @Test
    public void testPosReturnValueIteratorForEach() {
        final RowSet.Iterator it = new FakeIterator();
        assertTrue(it.forEachLong((v) -> true));
    }
}
