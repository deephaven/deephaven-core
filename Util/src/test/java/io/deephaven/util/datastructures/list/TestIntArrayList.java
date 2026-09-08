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

public class TestIntArrayList {

    @Test
    public void testEmpty() {
        final IntArrayList list = new IntArrayList();
        TestCase.assertEquals(0, list.size());
        list.clear();
        TestCase.assertEquals(0, list.size());
    }

    @Test
    public void testAddAndGet() {
        final IntArrayList list = new IntArrayList();
        list.add((int) 1);
        list.add((int) 2);
        list.add((int) 3);
        TestCase.assertEquals(3, list.size());
        TestCase.assertEquals((int) 1, list.getInt(0));
        TestCase.assertEquals((int) 2, list.getInt(1));
        TestCase.assertEquals((int) 3, list.getInt(2));
    }

    @Test
    public void testGrow() {
        final IntArrayList list = new IntArrayList();
        for (int i = 0; i < 100; ++i) {
            list.add((int) i);
        }
        TestCase.assertEquals(100, list.size());
        for (int i = 0; i < 100; ++i) {
            TestCase.assertEquals((int) i, list.getInt(i));
        }
    }

    @Test
    public void testSet() {
        final IntArrayList list = new IntArrayList();
        list.add((int) 10);
        list.add((int) 20);
        list.set(0, (int) 99);
        TestCase.assertEquals((int) 99, list.getInt(0));
        TestCase.assertEquals((int) 20, list.getInt(1));
    }

    @Test
    public void testRemoveLast() {
        final IntArrayList list = new IntArrayList();
        list.add((int) 1);
        list.add((int) 2);
        list.add((int) 3);
        list.removeInt(2);
        TestCase.assertEquals(2, list.size());
        TestCase.assertEquals((int) 1, list.getInt(0));
        TestCase.assertEquals((int) 2, list.getInt(1));
        list.add((int) 42);
        TestCase.assertEquals(3, list.size());
        TestCase.assertEquals((int) 42, list.getInt(2));
    }

    @Test
    public void testRemoveElements() {
        final IntArrayList list = new IntArrayList();
        list.add((int) 1);
        list.add((int) 2);
        list.add((int) 3);
        list.add((int) 4);
        list.removeElements(2, list.size());
        TestCase.assertEquals(2, list.size());
        TestCase.assertEquals((int) 1, list.getInt(0));
        TestCase.assertEquals((int) 2, list.getInt(1));
        list.add((int) 42);
        TestCase.assertEquals(3, list.size());
        TestCase.assertEquals((int) 42, list.getInt(2));
    }

    @Test
    public void testEquals() {
        final IntArrayList a = new IntArrayList();
        final IntArrayList b = new IntArrayList();
        TestCase.assertEquals(a, b);
        TestCase.assertEquals(a.hashCode(), b.hashCode());
        TestCase.assertEquals(a, a);
        TestCase.assertFalse(a.equals(null));
        TestCase.assertFalse(a.equals("not a list"));

        a.add((int) 1);
        a.add((int) 2);
        a.add((int) 3);
        TestCase.assertFalse(a.equals(b));

        b.add((int) 1);
        b.add((int) 2);
        b.add((int) 3);
        TestCase.assertEquals(a, b);
        TestCase.assertEquals(a.hashCode(), b.hashCode());

        b.add((int) 4);
        TestCase.assertFalse(a.equals(b));

        b.removeInt(3);
        b.set(2, (int) 99);
        TestCase.assertFalse(a.equals(b));

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
        TestCase.assertEquals(grown, fresh);
        TestCase.assertEquals(grown.hashCode(), fresh.hashCode());
    }

    @Test
    public void testClear() {
        final IntArrayList list = new IntArrayList();
        list.add((int) 1);
        list.add((int) 2);
        list.clear();
        TestCase.assertEquals(0, list.size());
        list.add((int) 99);
        TestCase.assertEquals(1, list.size());
        TestCase.assertEquals((int) 99, list.getInt(0));
    }
}
