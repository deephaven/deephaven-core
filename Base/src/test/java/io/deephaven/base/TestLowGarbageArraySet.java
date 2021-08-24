/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import java.util.Iterator;
import java.util.NoSuchElementException;

import io.deephaven.base.testing.SimpleTestSupport;
import junit.framework.TestCase;

// --------------------------------------------------------------------
/**
 * Tests for {@link LowGarbageArraySet}.
 */
public class TestLowGarbageArraySet extends TestCase {

    // ----------------------------------------------------------------
    public void testLowGarbageArraySet() throws Exception {

        // test construction, isEmpty
        LowGarbageArraySet<Object> lowGarbageArraySet = new LowGarbageArraySet<Object>();
        SimpleTestSupport.assertCollectionContainsExactly(lowGarbageArraySet);
        assertTrue(lowGarbageArraySet.isEmpty());

        // test add, isEmpty
        assertTrue(lowGarbageArraySet.add("A"));
        assertTrue(lowGarbageArraySet.add("B"));
        assertFalse(lowGarbageArraySet.add("A"));
        SimpleTestSupport.assertCollectionContainsExactly(lowGarbageArraySet, "A", "B");
        assertFalse(lowGarbageArraySet.isEmpty());

        // test contains
        assertTrue(lowGarbageArraySet.contains("B"));
        assertFalse(lowGarbageArraySet.contains("C"));

        // test remove
        assertTrue(lowGarbageArraySet.remove("A"));
        SimpleTestSupport.assertCollectionContainsExactly(lowGarbageArraySet, "B");

        assertFalse(lowGarbageArraySet.remove("c"));
        SimpleTestSupport.assertCollectionContainsExactly(lowGarbageArraySet, "B");

        // test add
        assertTrue(lowGarbageArraySet.add(null));
        assertTrue(lowGarbageArraySet.add("C"));
        assertFalse(lowGarbageArraySet.add(null));
        SimpleTestSupport.assertCollectionContainsExactly(lowGarbageArraySet, "C", "B", null);

        // test remove
        assertTrue(lowGarbageArraySet.remove("B"));
        assertTrue(lowGarbageArraySet.remove("C"));
        assertTrue(lowGarbageArraySet.remove(null));
        SimpleTestSupport.assertCollectionContainsExactly(lowGarbageArraySet);

        // test expansion
        Object[] integers = new Object[20];
        for (int nIndex = 0; nIndex < integers.length; nIndex++) {
            integers[nIndex] = nIndex;
            assertTrue(lowGarbageArraySet.add(integers[nIndex]));
        }
        SimpleTestSupport.assertCollectionContainsExactly(lowGarbageArraySet, integers);

        // test clear
        lowGarbageArraySet.clear();
        SimpleTestSupport.assertCollectionContainsExactly(lowGarbageArraySet);

        // test iterator hasNext, next
        assertTrue(lowGarbageArraySet.add("A"));
        assertTrue(lowGarbageArraySet.add("B"));
        assertTrue(lowGarbageArraySet.add("C"));
        Iterator<Object> iterator = lowGarbageArraySet.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("A", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("B", iterator.next());

        // test remove mid-set
        assertTrue(iterator.hasNext());
        iterator.remove();
        assertTrue(iterator.hasNext());
        assertEquals("C", iterator.next());

        // test iterator at end
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("expected iterator.next() past end of set to fail");
        } catch (NoSuchElementException e) {
        }

        // test remove beginning and end of set
        iterator = lowGarbageArraySet.iterator();
        try {
            iterator.remove();
            fail("expected iterator.remove() before beginning of set to fail");
        } catch (IllegalStateException e) {
        }
        assertTrue(iterator.hasNext());
        assertEquals("A", iterator.next());
        assertTrue(iterator.hasNext());
        iterator.remove();
        assertTrue(iterator.hasNext());
        try {
            iterator.remove();
            fail("expected iterator.remove() twice to fail");
        } catch (IllegalStateException e) {
        }
        assertEquals("C", iterator.next());
        assertFalse(iterator.hasNext());
        iterator.remove();
        assertFalse(iterator.hasNext());
        try {
            iterator.remove();
            fail("expected iterator.remove() twice to fail");
        } catch (IllegalStateException e) {
        }

        // test iterator properly reset
        assertTrue(lowGarbageArraySet.add("A"));
        iterator = lowGarbageArraySet.iterator();
        iterator.next();
        iterator = lowGarbageArraySet.iterator();
        try {
            iterator.remove();
            fail("expected iterator.remove() before beginning of set to fail");
        } catch (IllegalStateException e) {
        }


        // test clone
        LowGarbageArraySet<Object> lowGarbageArraySet2 = lowGarbageArraySet.clone();
        assertTrue(lowGarbageArraySet.add("B"));
        assertTrue(lowGarbageArraySet.contains("B"));
        assertFalse(lowGarbageArraySet2.contains("B"));
    }
}
