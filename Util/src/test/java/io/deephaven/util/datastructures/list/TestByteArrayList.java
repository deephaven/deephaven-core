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

public class TestByteArrayList {

    @Test
    public void testEmpty() {
        final ByteArrayList list = new ByteArrayList();
        assertEquals(0, list.size());
        list.clear();
        assertEquals(0, list.size());
    }

    @Test
    public void testAddAndGet() {
        final ByteArrayList list = new ByteArrayList();
        list.add((byte) 1);
        list.add((byte) 2);
        list.add((byte) 3);
        assertEquals(3, list.size());
        assertEquals((byte) 1, list.getByte(0));
        assertEquals((byte) 2, list.getByte(1));
        assertEquals((byte) 3, list.getByte(2));
    }

    @Test
    public void testGrow() {
        final ByteArrayList list = new ByteArrayList();
        for (int i = 0; i < 100; ++i) {
            list.add((byte) i);
        }
        assertEquals(100, list.size());
        for (int i = 0; i < 100; ++i) {
            assertEquals((byte) i, list.getByte(i));
        }
    }

    @Test
    public void testSet() {
        final ByteArrayList list = new ByteArrayList();
        list.add((byte) 10);
        list.add((byte) 20);
        list.set(0, (byte) 99);
        assertEquals((byte) 99, list.getByte(0));
        assertEquals((byte) 20, list.getByte(1));
    }

    @Test
    public void testRemoveLast() {
        final ByteArrayList list = new ByteArrayList();
        list.add((byte) 1);
        list.add((byte) 2);
        list.add((byte) 3);
        list.removeByte(2);
        assertEquals(2, list.size());
        assertEquals((byte) 1, list.getByte(0));
        assertEquals((byte) 2, list.getByte(1));
        list.add((byte) 42);
        assertEquals(3, list.size());
        assertEquals((byte) 42, list.getByte(2));
    }

    @Test
    public void testRemoveElements() {
        final ByteArrayList list = new ByteArrayList();
        list.add((byte) 1);
        list.add((byte) 2);
        list.add((byte) 3);
        list.add((byte) 4);
        list.removeElements(2, list.size());
        assertEquals(2, list.size());
        assertEquals((byte) 1, list.getByte(0));
        assertEquals((byte) 2, list.getByte(1));
        list.add((byte) 42);
        assertEquals(3, list.size());
        assertEquals((byte) 42, list.getByte(2));
    }

    @Test
    public void testEquals() {
        final ByteArrayList a = new ByteArrayList();
        final ByteArrayList b = new ByteArrayList();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a, a);
        assertFalse(a.equals(null));
        assertFalse(a.equals("not a list"));

        a.add((byte) 1);
        a.add((byte) 2);
        a.add((byte) 3);
        assertFalse(a.equals(b));

        b.add((byte) 1);
        b.add((byte) 2);
        b.add((byte) 3);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        b.add((byte) 4);
        assertFalse(a.equals(b));

        b.removeByte(3);
        b.set(2, (byte) 99);
        assertFalse(a.equals(b));

        // Equality must depend on size, not backing-array length: a list that
        // grew and then shrank should equal a freshly-built list with the same
        // logical contents.
        final ByteArrayList grown = new ByteArrayList();
        grown.add((byte) 1);
        grown.add((byte) 2);
        grown.add((byte) 3);
        grown.removeByte(2);
        final ByteArrayList fresh = new ByteArrayList();
        fresh.add((byte) 1);
        fresh.add((byte) 2);
        assertEquals(grown, fresh);
        assertEquals(grown.hashCode(), fresh.hashCode());
    }

    @Test
    public void testClear() {
        final ByteArrayList list = new ByteArrayList();
        list.add((byte) 1);
        list.add((byte) 2);
        list.clear();
        assertEquals(0, list.size());
        list.add((byte) 99);
        assertEquals(1, list.size());
        assertEquals((byte) 99, list.getByte(0));
    }
}
