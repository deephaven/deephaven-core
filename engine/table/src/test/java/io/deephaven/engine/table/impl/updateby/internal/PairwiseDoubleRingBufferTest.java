/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PairwiseFloatRingBufferTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.engine.table.impl.updateby.internal.PairwiseDoubleRingBuffer;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;
import org.junit.experimental.categories.Category;

import java.util.NoSuchElementException;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

@Category(OutOfBandTest.class)
public class PairwiseDoubleRingBufferTest extends TestCase {

    private void assertEmpty(PairwiseDoubleRingBuffer rb) {
        assertTrue(rb.isEmpty());
        assertEquals(0, rb.size());

        try {
            rb.pop();
            fail("queue should be empty");
        } catch (NoSuchElementException x) {
            // expected
        }

        try {
            rb.element();
            fail("queue should be empty");
        } catch (NoSuchElementException x) {
            // expected
        }
    }

    private void assertNotEmpty(PairwiseDoubleRingBuffer rb, int expectedSize, double expectedHead) {
        assertFalse(rb.isEmpty());
        assertEquals(expectedSize, rb.size());

        assertTrue(expectedHead == rb.peek(Long.MIN_VALUE));
        try {
            assertTrue(expectedHead == rb.element());
        } catch (NoSuchElementException x) {
            fail("queue should not be empty");
        }
    }

    private void assertAdd(PairwiseDoubleRingBuffer rb, double newHead, int expectedSize, double expectedHead) {
        rb.push(newHead);
        assertNotEmpty(rb, expectedSize, expectedHead);
    }

    private void assertRemove(PairwiseDoubleRingBuffer rb, int expectedSize, double expectedHead) {
        assertNotEmpty(rb, expectedSize, expectedHead);
        try {
            assertTrue(expectedHead == rb.pop());
        } catch (NoSuchElementException x) {
            fail("queue should not be empty");
        }
    }

    double A = 'A';
    double B = 'B';
    double C = 'C';
    double D = 'D';
    double E = 'E';
    double F = 'F';
    double G = 'G';

    public void testAddRemove() {

        PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(3, NULL_DOUBLE, Double::min);

        assertEmpty(rb);

        // move the head and tail off zero
        for (double i = 0; i < 1000; i++) {
            rb.push(i);
        }
        for (double i = 0; i < 1000; i++) {
            rb.pop();
        }

        assertEmpty(rb);

        assertAdd(rb, A, 1, A);
        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        // take this opportunity to test some front()/back() functions
        assertEquals(rb.front(), A);
        assertEquals(rb.front(0), A);
        assertEquals(rb.front(1), B);
        assertEquals(rb.front(2), C);
        assertEquals(rb.back(),C);
        assertEquals(rb.peekBack(NULL_DOUBLE),C);

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

     public void testGrowSimple() {
         PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(5, NULL_DOUBLE, Double::min);

         assertAdd(rb, A, 1, A);
         assertAdd(rb, B, 2, A);
         assertAdd(rb, C, 3, A);
         assertAdd(rb, D, 4, A);
         assertAdd(rb, E, 5, A);

         // this will grow; the elements are in a single contiguous block
         assertAdd(rb, F, 6, A);

         assertRemove(rb, 6, A);
         assertRemove(rb, 5, B);
         assertRemove(rb, 4, C);
         assertRemove(rb, 3, D);
         assertRemove(rb, 2, E);
         assertRemove(rb, 1, F);
         assertEmpty(rb);

         rb.pushEmptyValue();
         assertEquals(rb.front(), NULL_DOUBLE);
         try {
             rb.front(-1);
             fail("expected a NoSuchElement exception");
         } catch (NoSuchElementException x) {
             // expected
         }
         try {
             rb.front(5);
             fail("expected a NoSuchElement exception");
         } catch (NoSuchElementException x) {
             // expected
         }
         assertEquals(rb.poll(0.0f), NULL_DOUBLE);
     }

    public void testGrowComplex() {
        PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(5, NULL_DOUBLE, Double::min);

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

    public void testWhenEmpty() {
        PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(5, NULL_DOUBLE, Double::min);
        try {
            rb.back();
            fail("expected a NoSuchElement exception");
        } catch (NoSuchElementException x) {
            // expected
        }
        try {
            rb.pop();
            fail("expected a NoSuchElement exception");
        } catch (NoSuchElementException x) {
            // expected
        }
        try {
            rb.front();
            fail("expected a NoSuchElement exception");
        } catch (NoSuchElementException x) {
            // expected
        }
        assertEquals(rb.poll((double)-1), (double)-1);
        assertEquals(rb.peek((double)-1), (double)-1);
        assertEquals(rb.peekBack((double)-1), (double)-1);
    }

    public void testBack() {
        PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(5, NULL_DOUBLE, Double::min);

        assertAdd(rb, A, 1, A);
        assertEquals(rb.back(), A);

        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertEquals(rb.back(), C);
    }

    public void testBackTailIsZero() {
        PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(5, NULL_DOUBLE, Double::min);

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
        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(3, 0.0f, Double::sum)) {

            // move the head and tail off zero
            for (double i = 0; i < 1000; i++) {
                rb.push(i);
            }
            for (double i = 0; i < 1000; i++) {
                rb.pop();
            }

            for (double i = 0; i < 10_000; i++)
                rb.push(i);

            for (double i = 10_000; i < 1_000_000; i++) {
                rb.push(i);
                assertEquals(i - 10_000 + 1, rb.front(1));
                assertEquals(i - 10_000, rb.pop());
                assertEquals(rb.remaining(), rb.capacity() - rb.size());
            }
        }
    }

    public void testEvaluateMinLargeAmounts() {
        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(3, Double.MAX_VALUE, Double::min)) {
            for (double i = 0; i < 10_000; i++)
                rb.push(i);

            for (double i = 10_000; i < 1_000_000; i++) {
                rb.push(i);
                assertEquals(i - 10_000 + 1, rb.front(1));
                assertEquals(i - 10_000, rb.evaluate()); // front of queue is min
                assertEquals(i - 10_000, rb.pop());
            }
        }
    }

    public void testEvaluateMaxLargeAmounts() {
        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(3, Double.MIN_VALUE, Double::max)) {
            for (double i = 0; i < 10_000; i++)
                rb.push(i);

            for (double i = 10_000; i < 1_000_000; i++) {
                rb.push(i);
                assertEquals(i - 10_000 + 1, rb.front(1));
                assertEquals(i, rb.evaluate()); // last value added is max
                assertEquals(i - 10_000, rb.pop());
            }
        }
    }

    public void testEvaluateSumLargeAmounts() {
        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(3, 0.0f, Double::sum)) {

            double runningSum = 0.0f;

            for (double i = 0; i < 1_000; i++) {
                rb.push(i);
                runningSum += i;
            }

            // stay small enough to avoid doubleing errors but prove the concept
            for (double i = 1_000; i < 10_000; i++) {
                rb.push(i);
                runningSum += i; // add the current value
                assertEquals(i - 1_000 + 1, rb.front(1));

                assertEquals(runningSum, rb.evaluate());

                assertEquals(i - 1_000, rb.pop());
                runningSum -= i - 1_000; // remove the value 1_0000 ago

                assertEquals(runningSum, rb.evaluate());
            }
        }
    }

    public void testEvaluationEdgeCase() {
        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(3, -Double.MAX_VALUE, Double::max)) {
            // move the head and tail off zero
            for (double i = 0; i < 500; i++) {
                rb.push(i);
            }
            for (double i = 0; i < 500; i++) {
                rb.pop();
            }

            for (double i = 0; i < 100; i++) {
                rb.push(i);
            }
            assertEquals((double)99, rb.evaluate()); // last value added is max
        }
    }
}
