/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.ringbuffer;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.verify.AssertionFailure;
import junit.framework.TestCase;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertThrows;

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

        assertEquals(expectedHead, rb.peek(SENTINEL));
        try {
            assertEquals(expectedHead, rb.element());
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
        assertEquals(expectedHead, rb.poll(SENTINEL));
    }

    private void assertRemove(CharRingBuffer rb, int expectedSize, char expectedHead) {
        assertNotEmpty(rb, expectedSize, expectedHead);
        try {
            assertEquals(expectedHead, rb.remove());
        } catch (NoSuchElementException x) {
            fail("queue should not be empty");
        }
    }

    private void assertContents(CharRingBuffer rb, char... expectedData) {
        final char[] data = rb.getAll();
        assertEquals(data.length, expectedData.length);
        for (int ii = 0; ii < data.length; ii++) {
            assertEquals(data[ii], expectedData[ii]);
        }
    }

    private void assertArrayEquals(char[] data, char... expectedData) {
        assertEquals(data.length, expectedData.length);
        for (int ii = 0; ii < data.length; ii++) {
            assertEquals(data[ii], expectedData[ii]);
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
        assertContents(rb, A, B, C);

        assertRemove(rb, 3, A);
        assertContents(rb, B, C);

        assertRemove(rb, 2, B);
        assertContents(rb, C);

        assertRemove(rb, 1, C);
        assertContents(rb);

        assertEmpty(rb);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertContents(rb, A, B);

        assertRemove(rb, 2, A);
        assertContents(rb, B);

        assertAdd(rb, C, 2, B);
        assertContents(rb, B, C);

        assertRemove(rb, 2, B);
        assertContents(rb, C);

        assertRemove(rb, 1, C);
        assertContents(rb);

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
        assertContents(rb, A, B, C);

        assertAdd(rb, D, 4, A);
        assertAdd(rb, E, 5, A);
        assertAdd(rb, F, 6, A);
        assertContents(rb, A, B, C, D, E, F);

        assertRemove(rb, 6, A);
        assertRemove(rb, 5, B);
        assertRemove(rb, 4, C);
        assertRemove(rb, 3, D);
        assertRemove(rb, 2, E);
        assertRemove(rb, 1, F);
        assertContents(rb);
        assertEmpty(rb);
    }

    public void testOfferPoll() {
        CharRingBuffer rb = new CharRingBuffer(3);

        assertEmpty(rb);

        assertOffer(rb, A, 1, A);
        assertOffer(rb, B, 2, A);
        assertOffer(rb, C, 3, A);

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
        assertOffer(rb, D, 4, A);

        assertAdd(rb, E, 5, A); // need one add to grow it from 4 to 8
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
        CharRingBuffer rb = new CharRingBuffer(4);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertAdd(rb, D, 4, A);
        assertFull(rb);

        // remove one so head != 0
        assertRemove(rb, 4, A);

        assertAdd(rb, E, 4, B);
        // this will grow; the elements are in a single contiguous block
        assertAdd(rb, F, 5, B);

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

        final CharRingBuffer.Iterator iterFinal = rb.iterator();

        assertThrows(UnsupportedOperationException.class,
                iterFinal::remove);
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
        CharRingBuffer rb = new CharRingBuffer(5, false);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertAdd(rb, D, 4, A);
        assertAdd(rb, E, 5, A);

        assertRemove(rb, 5, A);
        assertAdd(rb, F, 5, B);
        assertEquals(rb.back(), F);
    }

    public void testLargeAmounts() {
        CharRingBuffer rb = new CharRingBuffer(3);

        for (int i = 0; i < 100; i++)
            rb.add((char) i);

        for (int i = 100; i < 200; i++) {
            rb.add((char) i);
            assertEquals((char) (i - 100 + 1), rb.front(1));
            assertEquals((char) (i - 100), rb.poll(SENTINEL));
        }

        for (int i = 200; i < 300; i++) {
            if (i < 299)
                assertEquals((char) (i - 100 + 1), rb.front(1));
            assertEquals((char) (i - 100), rb.poll(SENTINEL));
        }
    }

    public void testAddExceptionWhenFull() {
        CharRingBuffer rb = new CharRingBuffer(4, false);
        assert (rb.add(A));
        assert (rb.add(B));
        assert (rb.add(C));
        assert (rb.add(D));

        // this should throw
        assertThrows(UnsupportedOperationException.class,
                () -> rb.add(E));
    }

    public void testAddOverwriteAndOffer() {
        CharRingBuffer rb = new CharRingBuffer(4, false);
        assert (4 == rb.remaining());

        assert (F == rb.addOverwrite(A, F));
        assert (3 == rb.remaining());

        assert (F == rb.addOverwrite(B, F));
        assert (2 == rb.remaining());

        assert (F == rb.addOverwrite(C, F));
        assert (1 == rb.remaining());

        assert (F == rb.addOverwrite(D, F));
        assert (0 == rb.remaining());

        assert (rb.isFull());

        // now full, should return first value
        assert (A == rb.addOverwrite(E, F));
        assert (B == rb.addOverwrite(F, F));
        assert (rb.isFull());

        // offer() testing
        assert (!rb.offer(A));
        assert (C == rb.remove());
        assert (rb.offer(A));

        // peek testing
        assert (D == rb.front());
        assert (E == rb.front(1));
        assert (F == rb.front(2));
        // this should throw
        assertThrows(NoSuchElementException.class,
                () -> rb.front(99));

        assert (A == rb.peekBack(B));

        // clear() testing
        rb.clear();
        assert (rb.isEmpty());

        assert (A == rb.peekBack(A));
    }


    public void testMultipleRemove() {
        CharRingBuffer rb = new CharRingBuffer(10, false);

        // this should throw
        assertThrows(NoSuchElementException.class,
                rb::remove);

        // this should throw
        assertThrows(NoSuchElementException.class,
                () -> rb.remove(1));

        rb.add(A);
        rb.add(B);

        char[] values = rb.remove(2);
        assertArrayEquals(values, A, B);
        assertEmpty(rb);

        rb.add(C);
        rb.add(D);
        rb.add(E);
        rb.add(F);

        values = rb.remove(2);
        assertArrayEquals(values, C, D);

        values = rb.remove(2);
        assertArrayEquals(values, E, F);
        assertEmpty(rb);

        rb.add(A);
        rb.add(B);
        rb.add(C);
        rb.add(D);
        rb.add(E);
        rb.add(F);

        values = rb.remove(6);
        assertArrayEquals(values, A, B, C, D, E, F);
        assertEmpty(rb);
    }

    public void testAddRemoveUnsafe() {
        CharRingBuffer rbNoGrow = new CharRingBuffer(3, false);

        // this should throw
        assertThrows(UnsupportedOperationException.class,
                () -> rbNoGrow.ensureRemaining(10));

        rbNoGrow.ensureRemaining(3);
        rbNoGrow.addUnsafe(A);
        rbNoGrow.addUnsafe(B);
        rbNoGrow.addUnsafe(C);

        assertContents(rbNoGrow, A, B, C);

        assertEquals(rbNoGrow.removeUnsafe(), A);
        assertContents(rbNoGrow, B, C);

        assertEquals(rbNoGrow.removeUnsafe(), B);
        assertContents(rbNoGrow, C);

        assertEquals(rbNoGrow.removeUnsafe(), C);
        assertEmpty(rbNoGrow);


        CharRingBuffer rbGrow = new CharRingBuffer(3, true);

        for (int size = 10; size < 1_000_000; size *= 10) {
            rbGrow.ensureRemaining(size);
            assert (rbGrow.remaining() >= size);
            for (int i = 0; i < size; i++) {
                rbGrow.addUnsafe(A);
            }
        }
    }

    public void testOverflow() {
        CharRingBuffer rbA = new CharRingBuffer(0);
        // this should throw
        assertThrows(AssertionFailure.class,
                () -> rbA.ensureRemaining(ArrayUtil.MAX_ARRAY_SIZE + 1));

        CharRingBuffer rbB = new CharRingBuffer(100);
        for (int i = 0; i < 100; i++) {
            rbB.addUnsafe(A);
        }
        // this should throw
        assertThrows(AssertionFailure.class,
                () -> rbB.ensureRemaining(ArrayUtil.MAX_ARRAY_SIZE - 100 + 1));
    }
}
