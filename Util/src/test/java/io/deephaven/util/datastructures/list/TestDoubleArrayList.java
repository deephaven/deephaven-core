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

public class TestDoubleArrayList {

    @Test
    public void testEmpty() {
        final DoubleArrayList list = new DoubleArrayList();
        assertEquals(0, list.size());
        list.clear();
        assertEquals(0, list.size());
    }

    @Test
    public void testAddAndGet() {
        final DoubleArrayList list = new DoubleArrayList();
        list.add((double) 1);
        list.add((double) 2);
        list.add((double) 3);
        assertEquals(3, list.size());
        assertEquals((double) 1, list.getDouble(0));
        assertEquals((double) 2, list.getDouble(1));
        assertEquals((double) 3, list.getDouble(2));
    }

    @Test
    public void testGrow() {
        final DoubleArrayList list = new DoubleArrayList();
        for (int i = 0; i < 100; ++i) {
            list.add((double) i);
        }
        assertEquals(100, list.size());
        for (int i = 0; i < 100; ++i) {
            assertEquals((double) i, list.getDouble(i));
        }
    }

    @Test
    public void testSet() {
        final DoubleArrayList list = new DoubleArrayList();
        list.add((double) 10);
        list.add((double) 20);
        list.set(0, (double) 99);
        assertEquals((double) 99, list.getDouble(0));
        assertEquals((double) 20, list.getDouble(1));
    }

    @Test
    public void testRemoveLast() {
        final DoubleArrayList list = new DoubleArrayList();
        list.add((double) 1);
        list.add((double) 2);
        list.add((double) 3);
        list.removeDouble(2);
        assertEquals(2, list.size());
        assertEquals((double) 1, list.getDouble(0));
        assertEquals((double) 2, list.getDouble(1));
        list.add((double) 42);
        assertEquals(3, list.size());
        assertEquals((double) 42, list.getDouble(2));
    }

    @Test
    public void testRemoveElements() {
        final DoubleArrayList list = new DoubleArrayList();
        list.add((double) 1);
        list.add((double) 2);
        list.add((double) 3);
        list.add((double) 4);
        list.removeElements(2, list.size());
        assertEquals(2, list.size());
        assertEquals((double) 1, list.getDouble(0));
        assertEquals((double) 2, list.getDouble(1));
        list.add((double) 42);
        assertEquals(3, list.size());
        assertEquals((double) 42, list.getDouble(2));
    }

    @Test
    public void testEquals() {
        final DoubleArrayList a = new DoubleArrayList();
        final DoubleArrayList b = new DoubleArrayList();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a, a);
        assertFalse(a.equals(null));
        assertFalse(a.equals("not a list"));

        a.add((double) 1);
        a.add((double) 2);
        a.add((double) 3);
        assertFalse(a.equals(b));

        b.add((double) 1);
        b.add((double) 2);
        b.add((double) 3);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        b.add((double) 4);
        assertFalse(a.equals(b));

        b.removeDouble(3);
        b.set(2, (double) 99);
        assertFalse(a.equals(b));

        // Equality must depend on size, not backing-array length: a list that
        // grew and then shrank should equal a freshly-built list with the same
        // logical contents.
        final DoubleArrayList grown = new DoubleArrayList();
        grown.add((double) 1);
        grown.add((double) 2);
        grown.add((double) 3);
        grown.removeDouble(2);
        final DoubleArrayList fresh = new DoubleArrayList();
        fresh.add((double) 1);
        fresh.add((double) 2);
        assertEquals(grown, fresh);
        assertEquals(grown.hashCode(), fresh.hashCode());
    }

    @Test
    public void testClear() {
        final DoubleArrayList list = new DoubleArrayList();
        list.add((double) 1);
        list.add((double) 2);
        list.clear();
        assertEquals(0, list.size());
        list.add((double) 99);
        assertEquals(1, list.size());
        assertEquals((double) 99, list.getDouble(0));
    }
}
