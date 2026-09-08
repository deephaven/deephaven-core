//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharArrayList and run "./gradlew replicatePrimitiveArrayLists" to regenerate
//
// @formatter:off
package io.deephaven.util.datastructures.list;

import junit.framework.TestCase;
import org.junit.Test;

public class TestByteArrayList {

    @Test
    public void testEmpty() {
        final ByteArrayList list = new ByteArrayList();
        TestCase.assertEquals(0, list.size());
        list.clear();
        TestCase.assertEquals(0, list.size());
    }

    @Test
    public void testAddAndGet() {
        final ByteArrayList list = new ByteArrayList();
        list.add((byte) 1);
        list.add((byte) 2);
        list.add((byte) 3);
        TestCase.assertEquals(3, list.size());
        TestCase.assertEquals((byte) 1, list.getByte(0));
        TestCase.assertEquals((byte) 2, list.getByte(1));
        TestCase.assertEquals((byte) 3, list.getByte(2));
    }

    @Test
    public void testGrow() {
        final ByteArrayList list = new ByteArrayList();
        for (int i = 0; i < 100; ++i) {
            list.add((byte) i);
        }
        TestCase.assertEquals(100, list.size());
        for (int i = 0; i < 100; ++i) {
            TestCase.assertEquals((byte) i, list.getByte(i));
        }
    }

    @Test
    public void testSet() {
        final ByteArrayList list = new ByteArrayList();
        list.add((byte) 10);
        list.add((byte) 20);
        list.set(0, (byte) 99);
        TestCase.assertEquals((byte) 99, list.getByte(0));
        TestCase.assertEquals((byte) 20, list.getByte(1));
    }

    @Test
    public void testRemoveLast() {
        final ByteArrayList list = new ByteArrayList();
        list.add((byte) 1);
        list.add((byte) 2);
        list.add((byte) 3);
        list.removeByte(2);
        TestCase.assertEquals(2, list.size());
        TestCase.assertEquals((byte) 1, list.getByte(0));
        TestCase.assertEquals((byte) 2, list.getByte(1));
        list.add((byte) 42);
        TestCase.assertEquals(3, list.size());
        TestCase.assertEquals((byte) 42, list.getByte(2));
    }

    @Test
    public void testRemoveElements() {
        final ByteArrayList list = new ByteArrayList();
        list.add((byte) 1);
        list.add((byte) 2);
        list.add((byte) 3);
        list.add((byte) 4);
        list.removeElements(2, list.size());
        TestCase.assertEquals(2, list.size());
        TestCase.assertEquals((byte) 1, list.getByte(0));
        TestCase.assertEquals((byte) 2, list.getByte(1));
        list.add((byte) 42);
        TestCase.assertEquals(3, list.size());
        TestCase.assertEquals((byte) 42, list.getByte(2));
    }

    @Test
    public void testEquals() {
        final ByteArrayList a = new ByteArrayList();
        final ByteArrayList b = new ByteArrayList();
        TestCase.assertEquals(a, b);
        TestCase.assertEquals(a.hashCode(), b.hashCode());
        TestCase.assertEquals(a, a);
        TestCase.assertFalse(a.equals(null));
        TestCase.assertFalse(a.equals("not a list"));

        a.add((byte) 1);
        a.add((byte) 2);
        a.add((byte) 3);
        TestCase.assertFalse(a.equals(b));

        b.add((byte) 1);
        b.add((byte) 2);
        b.add((byte) 3);
        TestCase.assertEquals(a, b);
        TestCase.assertEquals(a.hashCode(), b.hashCode());

        b.add((byte) 4);
        TestCase.assertFalse(a.equals(b));

        b.removeByte(3);
        b.set(2, (byte) 99);
        TestCase.assertFalse(a.equals(b));

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
        TestCase.assertEquals(grown, fresh);
        TestCase.assertEquals(grown.hashCode(), fresh.hashCode());
    }

    @Test
    public void testClear() {
        final ByteArrayList list = new ByteArrayList();
        list.add((byte) 1);
        list.add((byte) 2);
        list.clear();
        TestCase.assertEquals(0, list.size());
        list.add((byte) 99);
        TestCase.assertEquals(1, list.size());
        TestCase.assertEquals((byte) 99, list.getByte(0));
    }
}
