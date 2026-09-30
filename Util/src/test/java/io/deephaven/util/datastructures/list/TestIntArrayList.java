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

public class TestIntArrayList {

    @Test
    public void testEmpty() {
        final IntArrayList list = new IntArrayList();
        assertEquals(0, list.size());
        list.clear();
        assertEquals(0, list.size());
    }

    @Test
    public void testAddAndGet() {
        final IntArrayList list = new IntArrayList();
        list.add((int) 1);
        list.add((int) 2);
        list.add((int) 3);
        assertEquals(3, list.size());
        assertEquals((int) 1, list.getInt(0));
        assertEquals((int) 2, list.getInt(1));
        assertEquals((int) 3, list.getInt(2));
    }

    @Test
    public void testGrow() {
        final IntArrayList list = new IntArrayList();
        for (int i = 0; i < 100; ++i) {
            list.add((int) i);
        }
        assertEquals(100, list.size());
        for (int i = 0; i < 100; ++i) {
            assertEquals((int) i, list.getInt(i));
        }
    }

    @Test
    public void testSet() {
        final IntArrayList list = new IntArrayList();
        list.add((int) 10);
        list.add((int) 20);
        list.set(0, (int) 99);
        assertEquals((int) 99, list.getInt(0));
        assertEquals((int) 20, list.getInt(1));
    }

    @Test
    public void testRemoveLast() {
        final IntArrayList list = new IntArrayList();
        list.add((int) 1);
        list.add((int) 2);
        list.add((int) 3);
        list.removeInt(2);
        assertEquals(2, list.size());
        assertEquals((int) 1, list.getInt(0));
        assertEquals((int) 2, list.getInt(1));
        list.add((int) 42);
        assertEquals(3, list.size());
        assertEquals((int) 42, list.getInt(2));
    }

    @Test
    public void testRemoveElements() {
        final IntArrayList list = new IntArrayList();
        list.add((int) 1);
        list.add((int) 2);
        list.add((int) 3);
        list.add((int) 4);
        list.removeElements(2, list.size());
        assertEquals(2, list.size());
        assertEquals((int) 1, list.getInt(0));
        assertEquals((int) 2, list.getInt(1));
        list.add((int) 42);
        assertEquals(3, list.size());
        assertEquals((int) 42, list.getInt(2));
    }

    @Test
    public void testEquals() {
        final IntArrayList a = new IntArrayList();
        final IntArrayList b = new IntArrayList();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a, a);
        assertFalse(a.equals(null));
        assertFalse(a.equals("not a list"));

        a.add((int) 1);
        a.add((int) 2);
        a.add((int) 3);
        assertFalse(a.equals(b));

        b.add((int) 1);
        b.add((int) 2);
        b.add((int) 3);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        b.add((int) 4);
        assertFalse(a.equals(b));

        b.removeInt(3);
        b.set(2, (int) 99);
        assertFalse(a.equals(b));

        // Equality must depend on size, not backing-array length: a list that
        // grew and then shrank should equal a freshly-built list with the same
        // logical contents.
        final IntArrayList grown = new IntArrayList();
        grown.add((int) 1);
        grown.add((int) 2);
        grown.add((int) 3);
        grown.removeInt(2);
        final IntArrayList fresh = new IntArrayList();
        fresh.add((int) 1);
        fresh.add((int) 2);
        assertEquals(grown, fresh);
        assertEquals(grown.hashCode(), fresh.hashCode());
    }

    @Test
    public void testClear() {
        final IntArrayList list = new IntArrayList();
        list.add((int) 1);
        list.add((int) 2);
        list.clear();
        assertEquals(0, list.size());
        list.add((int) 99);
        assertEquals(1, list.size());
        assertEquals((int) 99, list.getInt(0));
    }
}
