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

public class TestFloatArrayList {

    @Test
    public void testEmpty() {
        final FloatArrayList list = new FloatArrayList();
        assertEquals(0, list.size());
        list.clear();
        assertEquals(0, list.size());
    }

    @Test
    public void testAddAndGet() {
        final FloatArrayList list = new FloatArrayList();
        list.add((float) 1);
        list.add((float) 2);
        list.add((float) 3);
        assertEquals(3, list.size());
        assertEquals((float) 1, list.getFloat(0));
        assertEquals((float) 2, list.getFloat(1));
        assertEquals((float) 3, list.getFloat(2));
    }

    @Test
    public void testGrow() {
        final FloatArrayList list = new FloatArrayList();
        for (int i = 0; i < 100; ++i) {
            list.add((float) i);
        }
        assertEquals(100, list.size());
        for (int i = 0; i < 100; ++i) {
            assertEquals((float) i, list.getFloat(i));
        }
    }

    @Test
    public void testSet() {
        final FloatArrayList list = new FloatArrayList();
        list.add((float) 10);
        list.add((float) 20);
        list.set(0, (float) 99);
        assertEquals((float) 99, list.getFloat(0));
        assertEquals((float) 20, list.getFloat(1));
    }

    @Test
    public void testRemoveLast() {
        final FloatArrayList list = new FloatArrayList();
        list.add((float) 1);
        list.add((float) 2);
        list.add((float) 3);
        list.removeFloat(2);
        assertEquals(2, list.size());
        assertEquals((float) 1, list.getFloat(0));
        assertEquals((float) 2, list.getFloat(1));
        list.add((float) 42);
        assertEquals(3, list.size());
        assertEquals((float) 42, list.getFloat(2));
    }

    @Test
    public void testRemoveElements() {
        final FloatArrayList list = new FloatArrayList();
        list.add((float) 1);
        list.add((float) 2);
        list.add((float) 3);
        list.add((float) 4);
        list.removeElements(2, list.size());
        assertEquals(2, list.size());
        assertEquals((float) 1, list.getFloat(0));
        assertEquals((float) 2, list.getFloat(1));
        list.add((float) 42);
        assertEquals(3, list.size());
        assertEquals((float) 42, list.getFloat(2));
    }

    @Test
    public void testEquals() {
        final FloatArrayList a = new FloatArrayList();
        final FloatArrayList b = new FloatArrayList();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a, a);
        assertFalse(a.equals(null));
        assertFalse(a.equals("not a list"));

        a.add((float) 1);
        a.add((float) 2);
        a.add((float) 3);
        assertFalse(a.equals(b));

        b.add((float) 1);
        b.add((float) 2);
        b.add((float) 3);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        b.add((float) 4);
        assertFalse(a.equals(b));

        b.removeFloat(3);
        b.set(2, (float) 99);
        assertFalse(a.equals(b));

        // Equality must depend on size, not backing-array length: a list that
        // grew and then shrank should equal a freshly-built list with the same
        // logical contents.
        final FloatArrayList grown = new FloatArrayList();
        grown.add((float) 1);
        grown.add((float) 2);
        grown.add((float) 3);
        grown.removeFloat(2);
        final FloatArrayList fresh = new FloatArrayList();
        fresh.add((float) 1);
        fresh.add((float) 2);
        assertEquals(grown, fresh);
        assertEquals(grown.hashCode(), fresh.hashCode());
    }

    @Test
    public void testClear() {
        final FloatArrayList list = new FloatArrayList();
        list.add((float) 1);
        list.add((float) 2);
        list.clear();
        assertEquals(0, list.size());
        list.add((float) 99);
        assertEquals(1, list.size());
        assertEquals((float) 99, list.getFloat(0));
    }
}
