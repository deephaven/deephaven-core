/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import junit.framework.TestCase;

import java.util.NoSuchElementException;

public class RingBufferTest extends TestCase {

    private void assertEmpty(RingBuffer<Object> rb) {
        assertTrue(rb.isEmpty());
        assertEquals(0, rb.size());

        assertEquals(null, rb.peek());
        try {
            rb.element();
            fail("queue should be empty");
        } catch (NoSuchElementException x) {
            // expected
        }

        assertEquals(null, rb.poll());
        try {
            rb.remove();
            fail("queue should be empty");
        } catch (NoSuchElementException x) {
            // expected
        }

        assertEquals(null, rb.peekLast());
        try {
            rb.back();
            fail("queue should be empty");
        } catch (NoSuchElementException x) {
            // expected
        }
    }

    private void assertFull(RingBuffer<Object> rb) {
        assertFalse(rb.isEmpty());
        assertEquals(rb.capacity(), rb.size());
    }

    private void assertNotEmpty(RingBuffer<Object> rb, int expectedSize, Object expectedHead) {
        assertFalse(rb.isEmpty());
        assertEquals(expectedSize, rb.size());

        assertTrue(expectedHead == rb.peek());
        try {
            assertTrue(expectedHead == rb.element());
        } catch (NoSuchElementException x) {
            fail("queue should not be empty");
        }
    }

    private void assertAdd(RingBuffer<Object> rb, Object newElement, int expectedSize,
        Object expectedHead) {
        assertTrue(rb.add(newElement));
        assertEquals(newElement, rb.back());
        assertEquals(newElement, rb.peekLast());
        assertNotEmpty(rb, expectedSize, expectedHead);
    }

    private void assertAddOverwrite(RingBuffer<Object> rb, Object newElement, int expectedSize,
        Object expectedHead, Object expectedOverwrite) {
        assertEquals(expectedOverwrite, rb.addOverwrite(newElement));
        assertEquals(newElement, rb.back());
        assertEquals(newElement, rb.peekLast());
        assertNotEmpty(rb, expectedSize, expectedHead);
    }

    private void assertAddFirst(RingBuffer<Object> rb, Object newElement, int expectedSize) {
        assertTrue(rb.addFirst(newElement));
        assertEquals(newElement, rb.front());
        assertEquals(newElement, rb.peek());
        assertNotEmpty(rb, expectedSize, newElement);
    }

    private void assertOffer(RingBuffer<Object> rb, Object newElement, int expectedSize,
        Object expectedHead) {
        assertTrue(rb.offer(newElement));
        assertEquals(newElement, rb.back());
        assertEquals(newElement, rb.peekLast());
        assertNotEmpty(rb, expectedSize, expectedHead);
    }

    private void assertOfferFirst(RingBuffer<Object> rb, Object newElement, int expectedSize) {
        assertTrue(rb.offerFirst(newElement));
        assertEquals(newElement, rb.front());
        assertEquals(newElement, rb.peek());
        assertNotEmpty(rb, expectedSize, newElement);
    }

    private void assertPoll(RingBuffer<Object> rb, int expectedSize, Object expectedHead) {
        assertNotEmpty(rb, expectedSize, expectedHead);
        assertTrue(expectedHead == rb.poll());
    }

    private void assertRemove(RingBuffer<Object> rb, int expectedSize, Object expectedHead) {
        assertNotEmpty(rb, expectedSize, expectedHead);
        try {
            assertTrue(expectedHead == rb.remove());
        } catch (NoSuchElementException x) {
            fail("queue should not be empty");
        }
    }

    private void assertRemoveAtSwapLast(RingBuffer<Object> rb, int expectedSize,
        Object expectedHead, Object expectedResult, int offset) {
        assertNotEmpty(rb, expectedSize, expectedHead);
        try {
            assertTrue(expectedResult == rb.removeAtSwapLast(offset));
        } catch (NoSuchElementException x) {
            fail("queue should not be empty");
        }
    }

    private void assertContents(RingBuffer<Object> rb, Object... values) {
        for (int vi = 0; vi < values.length; ++vi) {
            assertEquals(rb.front(vi), values[vi]);
        }
    }

    Object A = new String("A");
    Object B = new String("B");
    Object C = new String("C");
    Object D = new String("D");
    Object E = new String("E");
    Object F = new String("F");
    Object G = new String("G");
    Object H = new String("H");

    public void testAddRemove() {

        RingBuffer<Object> rb = new RingBuffer<>(3);

        assertEmpty(rb);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertFull(rb);

        assertRemove(rb, 3, A);
        assertRemove(rb, 2, B);
        assertRemove(rb, 1, C);
        assertEmpty(rb);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertRemove(rb, 2, A);
        assertAdd(rb, C, 2, B);
        assertRemove(rb, 2, B);
        assertRemove(rb, 1, C);
        assertEmpty(rb);

        assertAdd(rb, A, 1, A);
        assertRemove(rb, 1, A);
        assertEmpty(rb);
        assertAdd(rb, B, 1, B);
        assertRemove(rb, 1, B);
        assertEmpty(rb);
        assertAdd(rb, C, 1, C);
        assertRemove(rb, 1, C);
        assertEmpty(rb);

        assertAdd(rb, D, 1, D);
        assertRemove(rb, 1, D);
        assertEmpty(rb);
        assertAdd(rb, E, 1, E);
        assertRemove(rb, 1, E);
        assertEmpty(rb);
        assertAdd(rb, F, 1, F);
        assertRemove(rb, 1, F);
        assertEmpty(rb);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertFull(rb);

        assertAdd(rb, D, 4, A);
        assertAdd(rb, E, 5, A);
        assertAdd(rb, F, 6, A);

        assertRemove(rb, 6, A);
        assertRemove(rb, 5, B);
        assertRemove(rb, 4, C);
        assertRemove(rb, 3, D);
        assertRemove(rb, 2, E);
        assertRemove(rb, 1, F);
        assertEmpty(rb);
    }

    public void testOfferPoll() {
        RingBuffer<Object> rb = new RingBuffer<>(3);

        assertEmpty(rb);

        assertOffer(rb, A, 1, A);
        assertOffer(rb, B, 2, A);
        assertOffer(rb, C, 3, A);

        assertFull(rb);

        assertPoll(rb, 3, A);
        assertPoll(rb, 2, B);
        assertPoll(rb, 1, C);
        assertEmpty(rb);

        assertOffer(rb, A, 1, A);
        assertOffer(rb, B, 2, A);
        assertPoll(rb, 2, A);
        assertOffer(rb, C, 2, B);
        assertPoll(rb, 2, B);
        assertPoll(rb, 1, C);
        assertEmpty(rb);

        assertOffer(rb, A, 1, A);
        assertPoll(rb, 1, A);
        assertEmpty(rb);
        assertOffer(rb, B, 1, B);
        assertPoll(rb, 1, B);
        assertEmpty(rb);
        assertOffer(rb, C, 1, C);
        assertPoll(rb, 1, C);
        assertEmpty(rb);

        assertOffer(rb, D, 1, D);
        assertPoll(rb, 1, D);
        assertEmpty(rb);
        assertOffer(rb, E, 1, E);
        assertPoll(rb, 1, E);
        assertEmpty(rb);
        assertOffer(rb, F, 1, F);
        assertPoll(rb, 1, F);
        assertEmpty(rb);

        assertOffer(rb, A, 1, A);
        assertOffer(rb, B, 2, A);
        assertOffer(rb, C, 3, A);
        assertFull(rb);

        assertAdd(rb, D, 4, A); // need one add to grow it
        assertOffer(rb, E, 5, A); // NOTE: assumes capacity grows by at least a factor of two
        assertOffer(rb, F, 6, A);

        assertPoll(rb, 6, A);
        assertPoll(rb, 5, B);
        assertPoll(rb, 4, C);
        assertPoll(rb, 3, D);
        assertPoll(rb, 2, E);
        assertPoll(rb, 1, F);
        assertEmpty(rb);
    }

    public void testGrowSimple() {
        // In order to keep internal storage size as a power of 2, the following now applies:
        // capacity = 2^ceil(log2(requestedCapacity+1)) - 1
        RingBuffer<Object> rb = new RingBuffer<>(5);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertAdd(rb, D, 4, A);
        assertAdd(rb, E, 5, A);
        assertAdd(rb, F, 6, A);
        assertAdd(rb, G, 7, A);
        assertFull(rb);

        // this will grow; the elements are in a single contiguous block
        assertAdd(rb, H, 8, A);

        assertRemove(rb, 8, A);
        assertRemove(rb, 7, B);
        assertRemove(rb, 6, C);
        assertRemove(rb, 5, D);
        assertRemove(rb, 4, E);
        assertRemove(rb, 3, F);
        assertRemove(rb, 2, G);
        assertRemove(rb, 1, H);
        assertEmpty(rb);
    }

    public void testGrowComplex() {
        RingBuffer<Object> rb = new RingBuffer<>(5);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertRemove(rb, 3, A);
        assertRemove(rb, 2, B);
        assertRemove(rb, 1, C);
        assertEmpty(rb);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertAdd(rb, D, 4, A);
        assertAdd(rb, E, 5, A);
        assertAdd(rb, F, 6, A);
        assertAdd(rb, G, 7, A);
        assertFull(rb);

        // this will grow; the elements are in two blocks
        assertAdd(rb, H, 8, A);

        assertRemove(rb, 8, A);
        assertRemove(rb, 7, B);
        assertRemove(rb, 6, C);
        assertRemove(rb, 5, D);
        assertRemove(rb, 4, E);
        assertRemove(rb, 3, F);
        assertRemove(rb, 2, G);
        assertRemove(rb, 1, H);
        assertEmpty(rb);
    }

    public void testCircularArrayList() {
        RingBuffer<Object> rb = new RingBuffer<>(3);

        assertEmpty(rb);

        assertOfferFirst(rb, A, 1);
        assertOffer(rb, B, 2, A);
        assertOfferFirst(rb, C, 3);
        assertContents(rb, C, A, B);
        assertFull(rb);

        assertRemoveAtSwapLast(rb, 3, C, A, 1);
        assertContents(rb, C, B);
        assertRemoveAtSwapLast(rb, 2, C, C, 0);
        assertContents(rb, B);
        assertRemoveAtSwapLast(rb, 1, B, B, 0);
        assertEmpty(rb);

        assertAddFirst(rb, A, 1);
        assertAdd(rb, B, 2, A);
        assertAddFirst(rb, C, 3);
        assertAdd(rb, D, 4, C);
        assertAddFirst(rb, E, 5);
        assertAdd(rb, F, 6, E);
        assertAddFirst(rb, G, 7);
        assertAdd(rb, H, 8, G);
        assertContents(rb, G, E, C, A, B, D, F, H);

        assertRemoveAtSwapLast(rb, 8, G, H, 7);
        assertContents(rb, G, E, C, A, B, D, F);
        assertRemoveAtSwapLast(rb, 7, G, A, 3);
        assertContents(rb, G, E, C, F, B, D);
    }

    public void testOverwrite() {
        RingBuffer<Object> rb = new RingBuffer<>(3); // means array will be size 4, 1 always needs
                                                     // to be null
        assertAddOverwrite(rb, A, 1, A, null);
        assertAddOverwrite(rb, B, 2, A, null);
        assertAddOverwrite(rb, C, 3, A, null);
        assertAddOverwrite(rb, D, 3, B, A);
        assertAddOverwrite(rb, E, 3, C, B);
        assertAddOverwrite(rb, F, 3, D, C);
        assertAddOverwrite(rb, G, 3, E, D);
        assertAddOverwrite(rb, H, 3, F, E);
    }

    public void testPeekLast() {
        RingBuffer<Object> rb = new RingBuffer<>(7); // means array will be size 8, 1 always needs
                                                     // to be null
        assertOffer(rb, A, 1, A);
        assertOffer(rb, B, 2, A);
        assertOffer(rb, C, 3, A);
        assertOffer(rb, D, 4, A);
        assertOffer(rb, E, 5, A);
        assertOffer(rb, F, 6, A);
        assertOffer(rb, G, 7, A);

        assertEquals(rb.peekLast(0), G);
        assertEquals(rb.peekLast(1), F);
        assertEquals(rb.peekLast(2), E);
        assertEquals(rb.peekLast(3), D);
        assertEquals(rb.peekLast(4), C);
        assertEquals(rb.peekLast(5), B);
        assertEquals(rb.peekLast(6), A);

    }
}
