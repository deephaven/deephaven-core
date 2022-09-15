/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.ringbuffer;

import junit.framework.TestCase;

import java.util.NoSuchElementException;

public class CharRingBufferTest extends TestCase {

    final char SENTINEL = Character.MIN_VALUE;

    private void assertEmpty(CharRingBuffer rb) {
        assertTrue(rb.isEmpty());
        assertEquals(0, rb.size());

        assertEquals(SENTINEL, rb.peek(SENTINEL));
        try {
            rb.element();
            fail("queue should be empty");
        } catch (NoSuchElementException x) {
            // expected
        }

        assertEquals(SENTINEL, rb.poll(SENTINEL));
        try {
            rb.remove();
            fail("queue should be empty");
        } catch (NoSuchElementException x) {
            // expected
        }
    }

    private void assertFull(CharRingBuffer rb) {
        assertFalse(rb.isEmpty());
        assertEquals(rb.capacity(), rb.size());
    }

    private void assertNotEmpty(CharRingBuffer rb, int expectedSize, char expectedHead) {
        assertFalse(rb.isEmpty());
        assertEquals(expectedSize, rb.size());

        assertTrue(expectedHead == rb.peek(SENTINEL));
        try {
            assertTrue(expectedHead == rb.element());
        } catch (NoSuchElementException x) {
            fail("queue should not be empty");
        }
    }

    private void assertAdd(CharRingBuffer rb, char newHead, int expectedSize, char expectedHead) {
        assertTrue(rb.add(newHead));
        assertNotEmpty(rb, expectedSize, expectedHead);
    }

    private void assertOffer(CharRingBuffer rb, char newHead, int expectedSize, char expectedHead) {
        assertTrue(rb.offer(newHead));
        assertNotEmpty(rb, expectedSize, expectedHead);
    }

    private void assertPoll(CharRingBuffer rb, int expectedSize, char expectedHead) {
        assertNotEmpty(rb, expectedSize, expectedHead);
        assertTrue(expectedHead == rb.poll(SENTINEL));
    }

    private void assertRemove(CharRingBuffer rb, int expectedSize, char expectedHead) {
        assertNotEmpty(rb, expectedSize, expectedHead);
        try {
            assertTrue(expectedHead == rb.remove());
        } catch (NoSuchElementException x) {
            fail("queue should not be empty");
        }
    }

    char A = 'A';
    char B = 'B';
    char C = 'C';
    char D = 'D';
    char E = 'E';
    char F = 'F';

    public void testAddRemove() {

        CharRingBuffer rb = new CharRingBuffer(3);

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
        CharRingBuffer rb = new CharRingBuffer(3);

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
        CharRingBuffer rb = new CharRingBuffer(5);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertAdd(rb, D, 4, A);
        assertAdd(rb, E, 5, A);
        assertFull(rb);

        // this will grow; the elements are in a single contiguous block
        assertAdd(rb, F, 6, A);

        assertRemove(rb, 6, A);
        assertRemove(rb, 5, B);
        assertRemove(rb, 4, C);
        assertRemove(rb, 3, D);
        assertRemove(rb, 2, E);
        assertRemove(rb, 1, F);
        assertEmpty(rb);
    }

    public void testGrowComplex() {
        CharRingBuffer rb = new CharRingBuffer(5);

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
        assertFull(rb);

        // this will grow; the elements are in two blocks
        assertAdd(rb, F, 6, A);

        assertRemove(rb, 6, A);
        assertRemove(rb, 5, B);
        assertRemove(rb, 4, C);
        assertRemove(rb, 3, D);
        assertRemove(rb, 2, E);
        assertRemove(rb, 1, F);
        assertEmpty(rb);
    }

    public void testIterator() {
        CharRingBuffer rb = new CharRingBuffer(3);

        CharRingBuffer.Iterator iter = rb.iterator();
        assertFalse(iter.hasNext());

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertFull(rb);

        iter = rb.iterator();
        assertTrue(iter.hasNext());
        assertEquals(A, iter.next());
        assertTrue(iter.hasNext());
        assertEquals(B, iter.next());
        assertTrue(iter.hasNext());
        assertEquals(C, iter.next());
        assertFalse(iter.hasNext());

        assertRemove(rb, 3, A);
        assertAdd(rb, D, 3, B);

        iter = rb.iterator();
        assertTrue(iter.hasNext());
        assertEquals(B, iter.next());
        assertTrue(iter.hasNext());
        assertEquals(C, iter.next());
        assertTrue(iter.hasNext());
        assertEquals(D, iter.next());
        assertFalse(iter.hasNext());

        assertRemove(rb, 3, B);

        iter = rb.iterator();
        assertEquals(C, iter.next());
        assertEquals(D, iter.next());
        assertFalse(iter.hasNext());

        assertAdd(rb, E, 3, C);
        assertAdd(rb, F, 4, C);

        iter = rb.iterator();
        assertTrue(iter.hasNext());
        assertEquals(C, iter.next());
        assertTrue(iter.hasNext());
        assertEquals(D, iter.next());
        assertTrue(iter.hasNext());
        assertEquals(E, iter.next());
        assertTrue(iter.hasNext());
        assertEquals(F, iter.next());
        assertFalse(iter.hasNext());
    }

    public void testBack() {
        CharRingBuffer rb = new CharRingBuffer(5);

        assertAdd(rb, A, 1, A);
        assertEquals(rb.back(), A);

        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertEquals(rb.back(), C);
    }

    public void testBackWhenEmpty() {
        CharRingBuffer rb = new CharRingBuffer(5);
        try {
            rb.back();
            fail("expected a NoSuchElement exception");
        } catch (NoSuchElementException x) {
            // expected
        }
    }

    public void testBackTailIsZero() {
        CharRingBuffer rb = new CharRingBuffer(5);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertAdd(rb, D, 4, A);
        assertAdd(rb, E, 5, A);
        assertFull(rb);

        assertRemove(rb, 5, A);
        assertAdd(rb, F, 5, B);
        assertEquals(rb.back(), F);
    }

    public void testLargeAmounts() {
        CharRingBuffer rb = new CharRingBuffer(3);

        for (int i = 0; i < 100; i++)
            rb.add((char)i);

        for (int i = 100; i < 200; i++) {
            rb.add((char)i);
            assertEquals((char)(i - 100 + 1), rb.front(1));
            assertEquals((char)(i - 100), rb.poll(SENTINEL));
        }

        for (int i = 200; i < 300; i++) {
            if (i < 299)
                assertEquals((char)(i - 100 + 1), rb.front(1));
            assertEquals((char)(i - 100), rb.poll(SENTINEL));
        }
    }
}
