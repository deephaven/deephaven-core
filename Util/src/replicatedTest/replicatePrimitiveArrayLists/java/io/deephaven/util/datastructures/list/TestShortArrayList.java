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

public class TestShortArrayList {

    @Test
    public void testEmpty() {
        final ShortArrayList list = new ShortArrayList();
        assertEquals(0, list.size());
        list.clear();
        assertEquals(0, list.size());
    }

    @Test
    public void testAddAndGet() {
        final ShortArrayList list = new ShortArrayList();
        list.add((short) 1);
        list.add((short) 2);
        list.add((short) 3);
        assertEquals(3, list.size());
        assertEquals((short) 1, list.getShort(0));
        assertEquals((short) 2, list.getShort(1));
        assertEquals((short) 3, list.getShort(2));
    }

    @Test
    public void testGrow() {
        final ShortArrayList list = new ShortArrayList();
        for (int i = 0; i < 100; ++i) {
            list.add((short) i);
        }
        assertEquals(100, list.size());
        for (int i = 0; i < 100; ++i) {
            assertEquals((short) i, list.getShort(i));
        }
    }

    @Test
    public void testSet() {
        final ShortArrayList list = new ShortArrayList();
        list.add((short) 10);
        list.add((short) 20);
        list.set(0, (short) 99);
        assertEquals((short) 99, list.getShort(0));
        assertEquals((short) 20, list.getShort(1));
    }

    @Test
    public void testRemoveLast() {
        final ShortArrayList list = new ShortArrayList();
        list.add((short) 1);
        list.add((short) 2);
        list.add((short) 3);
        list.removeShort(2);
        assertEquals(2, list.size());
        assertEquals((short) 1, list.getShort(0));
        assertEquals((short) 2, list.getShort(1));
        list.add((short) 42);
        assertEquals(3, list.size());
        assertEquals((short) 42, list.getShort(2));
    }

    @Test
    public void testRemoveElements() {
        final ShortArrayList list = new ShortArrayList();
        list.add((short) 1);
        list.add((short) 2);
        list.add((short) 3);
        list.add((short) 4);
        list.removeElements(2, list.size());
        assertEquals(2, list.size());
        assertEquals((short) 1, list.getShort(0));
        assertEquals((short) 2, list.getShort(1));
        list.add((short) 42);
        assertEquals(3, list.size());
        assertEquals((short) 42, list.getShort(2));
    }

    @Test
    public void testEquals() {
        final ShortArrayList a = new ShortArrayList();
        final ShortArrayList b = new ShortArrayList();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a, a);
        assertFalse(a.equals(null));
        assertFalse(a.equals("not a list"));

        a.add((short) 1);
        a.add((short) 2);
        a.add((short) 3);
        assertFalse(a.equals(b));

        b.add((short) 1);
        b.add((short) 2);
        b.add((short) 3);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        b.add((short) 4);
        assertFalse(a.equals(b));

        b.removeShort(3);
        b.set(2, (short) 99);
        assertFalse(a.equals(b));

        // Equality must depend on size, not backing-array length: a list that
        // grew and then shrank should equal a freshly-built list with the same
        // logical contents.
        final ShortArrayList grown = new ShortArrayList();
        grown.add((short) 1);
        grown.add((short) 2);
        grown.add((short) 3);
        grown.removeShort(2);
        final ShortArrayList fresh = new ShortArrayList();
        fresh.add((short) 1);
        fresh.add((short) 2);
        assertEquals(grown, fresh);
        assertEquals(grown.hashCode(), fresh.hashCode());
    }

    @Test
    public void testClear() {
        final ShortArrayList list = new ShortArrayList();
        list.add((short) 1);
        list.add((short) 2);
        list.clear();
        assertEquals(0, list.size());
        list.add((short) 99);
        assertEquals(1, list.size());
        assertEquals((short) 99, list.getShort(0));
    }
}
