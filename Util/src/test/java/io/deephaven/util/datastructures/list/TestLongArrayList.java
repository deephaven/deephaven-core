//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharArrayList and run "./gradlew replicatePrimitiveArrayLists" to regenerate
//
// @formatter:off
package io.deephaven.util.datastructures.list;

import org.junit.Test;

import static io.deephaven.base.testing.Asserts.assertEquals;
import static org.junit.Assert.*;

public class TestLongArrayList {

    @Test
    public void testEmpty() {
        final LongArrayList list = new LongArrayList();
        assertEquals(0, list.size());
        list.clear();
        assertEquals(0, list.size());
    }

    @Test
    public void testAddAndGet() {
        final LongArrayList list = new LongArrayList();
        list.add((long) 1);
        list.add((long) 2);
        list.add((long) 3);
        assertEquals(3, list.size());
        assertEquals((long) 1, list.getLong(0));
        assertEquals((long) 2, list.getLong(1));
        assertEquals((long) 3, list.getLong(2));
    }

    @Test
    public void testGrow() {
        final LongArrayList list = new LongArrayList();
        for (int i = 0; i < 100; ++i) {
            list.add((long) i);
        }
        assertEquals(100, list.size());
        for (int i = 0; i < 100; ++i) {
            assertEquals((long) i, list.getLong(i));
        }
    }

    @Test
    public void testSet() {
        final LongArrayList list = new LongArrayList();
        list.add((long) 10);
        list.add((long) 20);
        list.set(0, (long) 99);
        assertEquals((long) 99, list.getLong(0));
        assertEquals((long) 20, list.getLong(1));
    }

    @Test
    public void testRemoveLast() {
        final LongArrayList list = new LongArrayList();
        list.add((long) 1);
        list.add((long) 2);
        list.add((long) 3);
        list.removeLong(2);
        assertEquals(2, list.size());
        assertEquals((long) 1, list.getLong(0));
        assertEquals((long) 2, list.getLong(1));
        list.add((long) 42);
        assertEquals(3, list.size());
        assertEquals((long) 42, list.getLong(2));
    }

    @Test
    public void testRemoveElements() {
        final LongArrayList list = new LongArrayList();
        list.add((long) 1);
        list.add((long) 2);
        list.add((long) 3);
        list.add((long) 4);
        list.removeElements(2, list.size());
        assertEquals(2, list.size());
        assertEquals((long) 1, list.getLong(0));
        assertEquals((long) 2, list.getLong(1));
        list.add((long) 42);
        assertEquals(3, list.size());
        assertEquals((long) 42, list.getLong(2));
    }

    @Test
    public void testEquals() {
        final LongArrayList a = new LongArrayList();
        final LongArrayList b = new LongArrayList();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a, a);
        assertFalse(a.equals(null));
        assertFalse(a.equals("not a list"));

        a.add((long) 1);
        a.add((long) 2);
        a.add((long) 3);
        assertFalse(a.equals(b));

        b.add((long) 1);
        b.add((long) 2);
        b.add((long) 3);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        b.add((long) 4);
        assertFalse(a.equals(b));

        b.removeLong(3);
        b.set(2, (long) 99);
        assertFalse(a.equals(b));

        // Equality must depend on size, not backing-array length: a list that
        // grew and then shrank should equal a freshly-built list with the same
        // logical contents.
        final LongArrayList grown = new LongArrayList();
        grown.add((long) 1);
        grown.add((long) 2);
        grown.add((long) 3);
        grown.removeLong(2);
        final LongArrayList fresh = new LongArrayList();
        fresh.add((long) 1);
        fresh.add((long) 2);
        assertEquals(grown, fresh);
        assertEquals(grown.hashCode(), fresh.hashCode());
    }

    @Test
    public void testClear() {
        final LongArrayList list = new LongArrayList();
        list.add((long) 1);
        list.add((long) 2);
        list.clear();
        assertEquals(0, list.size());
        list.add((long) 99);
        assertEquals(1, list.size());
        assertEquals((long) 99, list.getLong(0));
    }
}
