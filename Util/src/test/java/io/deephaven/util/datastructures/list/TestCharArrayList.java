//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.list;

import junit.framework.TestCase;
import org.junit.Test;

public class TestCharArrayList {

    @Test
    public void testEmpty() {
        final CharArrayList list = new CharArrayList();
        TestCase.assertEquals(0, list.size());
        list.clear();
        TestCase.assertEquals(0, list.size());
    }

    @Test
    public void testAddAndGet() {
        final CharArrayList list = new CharArrayList();
        list.add((char) 1);
        list.add((char) 2);
        list.add((char) 3);
        TestCase.assertEquals(3, list.size());
        TestCase.assertEquals((char) 1, list.getChar(0));
        TestCase.assertEquals((char) 2, list.getChar(1));
        TestCase.assertEquals((char) 3, list.getChar(2));
    }

    @Test
    public void testGrow() {
        final CharArrayList list = new CharArrayList();
        for (int i = 0; i < 100; ++i) {
            list.add((char) i);
        }
        TestCase.assertEquals(100, list.size());
        for (int i = 0; i < 100; ++i) {
            TestCase.assertEquals((char) i, list.getChar(i));
        }
    }

    @Test
    public void testSet() {
        final CharArrayList list = new CharArrayList();
        list.add((char) 10);
        list.add((char) 20);
        list.set(0, (char) 99);
        TestCase.assertEquals((char) 99, list.getChar(0));
        TestCase.assertEquals((char) 20, list.getChar(1));
    }

    @Test
    public void testRemoveLast() {
        final CharArrayList list = new CharArrayList();
        list.add((char) 1);
        list.add((char) 2);
        list.add((char) 3);
        list.removeChar(2);
        TestCase.assertEquals(2, list.size());
        TestCase.assertEquals((char) 1, list.getChar(0));
        TestCase.assertEquals((char) 2, list.getChar(1));
        list.add((char) 42);
        TestCase.assertEquals(3, list.size());
        TestCase.assertEquals((char) 42, list.getChar(2));
    }

    @Test
    public void testRemoveElements() {
        final CharArrayList list = new CharArrayList();
        list.add((char) 1);
        list.add((char) 2);
        list.add((char) 3);
        list.add((char) 4);
        list.removeElements(2, list.size());
        TestCase.assertEquals(2, list.size());
        TestCase.assertEquals((char) 1, list.getChar(0));
        TestCase.assertEquals((char) 2, list.getChar(1));
        list.add((char) 42);
        TestCase.assertEquals(3, list.size());
        TestCase.assertEquals((char) 42, list.getChar(2));
    }

    @Test
    public void testEquals() {
        final CharArrayList a = new CharArrayList();
        final CharArrayList b = new CharArrayList();
        TestCase.assertEquals(a, b);
        TestCase.assertEquals(a.hashCode(), b.hashCode());
        TestCase.assertEquals(a, a);
        TestCase.assertFalse(a.equals(null));
        TestCase.assertFalse(a.equals("not a list"));

        a.add((char) 1);
        a.add((char) 2);
        a.add((char) 3);
        TestCase.assertFalse(a.equals(b));

        b.add((char) 1);
        b.add((char) 2);
        b.add((char) 3);
        TestCase.assertEquals(a, b);
        TestCase.assertEquals(a.hashCode(), b.hashCode());

        b.add((char) 4);
        TestCase.assertFalse(a.equals(b));

        b.removeChar(3);
        b.set(2, (char) 99);
        TestCase.assertFalse(a.equals(b));

        // Equality must depend on size, not backing-array length: a list that
        // grew and then shrank should equal a freshly-built list with the same
        // logical contents.
        final CharArrayList grown = new CharArrayList();
        grown.add((char) 1);
        grown.add((char) 2);
        grown.add((char) 3);
        grown.removeChar(2);
        final CharArrayList fresh = new CharArrayList();
        fresh.add((char) 1);
        fresh.add((char) 2);
        TestCase.assertEquals(grown, fresh);
        TestCase.assertEquals(grown.hashCode(), fresh.hashCode());
    }

    @Test
    public void testClear() {
        final CharArrayList list = new CharArrayList();
        list.add((char) 1);
        list.add((char) 2);
        list.clear();
        TestCase.assertEquals(0, list.size());
        list.add((char) 99);
        TestCase.assertEquals(1, list.size());
        TestCase.assertEquals((char) 99, list.getChar(0));
    }
}
