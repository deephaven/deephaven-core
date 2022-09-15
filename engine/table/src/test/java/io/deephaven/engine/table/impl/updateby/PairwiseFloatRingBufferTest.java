/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.engine.table.impl.updateby.internal.PairwiseFloatRingBuffer;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;
import org.junit.experimental.categories.Category;

import java.util.NoSuchElementException;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

@Category(OutOfBandTest.class)
public class PairwiseFloatRingBufferTest extends TestCase {

    private void assertEmpty(PairwiseFloatRingBuffer rb) {
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

    private void assertNotEmpty(PairwiseFloatRingBuffer rb, int expectedSize, float expectedHead) {
        assertFalse(rb.isEmpty());
        assertEquals(expectedSize, rb.size());

        assertTrue(expectedHead == rb.peek(Long.MIN_VALUE));
        try {
            assertTrue(expectedHead == rb.element());
        } catch (NoSuchElementException x) {
            fail("queue should not be empty");
        }
    }

    private void assertAdd(PairwiseFloatRingBuffer rb, float newHead, int expectedSize, float expectedHead) {
        rb.push(newHead);
        assertNotEmpty(rb, expectedSize, expectedHead);
    }

    private void assertRemove(PairwiseFloatRingBuffer rb, int expectedSize, float expectedHead) {
        assertNotEmpty(rb, expectedSize, expectedHead);
        try {
            assertTrue(expectedHead == rb.pop());
        } catch (NoSuchElementException x) {
            fail("queue should not be empty");
        }
    }

    float A = 'A';
    float B = 'B';
    float C = 'C';
    float D = 'D';
    float E = 'E';
    float F = 'F';
    float G = 'G';

    public void testAddRemove() {

        PairwiseFloatRingBuffer rb = new PairwiseFloatRingBuffer(3, NULL_FLOAT, Float::min);

        assertEmpty(rb);

        // move the head and tail off zero
        for (float i = 0; i < 1000; i++) {
            rb.push(i);
        }
        for (float i = 0; i < 1000; i++) {
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
        assertEquals(rb.peekBack(NULL_FLOAT),C);

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
         PairwiseFloatRingBuffer rb = new PairwiseFloatRingBuffer(5, NULL_FLOAT, Float::min);

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
         assertEquals(rb.front(), NULL_FLOAT);
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
         assertEquals(rb.poll(0.0f), NULL_FLOAT);
     }

    public void testGrowComplex() {
        PairwiseFloatRingBuffer rb = new PairwiseFloatRingBuffer(5, NULL_FLOAT, Float::min);

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
        PairwiseFloatRingBuffer rb = new PairwiseFloatRingBuffer(5, NULL_FLOAT, Float::min);
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
        assertEquals(rb.poll((float)-1), (float)-1);
        assertEquals(rb.peek((float)-1), (float)-1);
        assertEquals(rb.peekBack((float)-1), (float)-1);
    }

    public void testBack() {
        PairwiseFloatRingBuffer rb = new PairwiseFloatRingBuffer(5, NULL_FLOAT, Float::min);

        assertAdd(rb, A, 1, A);
        assertEquals(rb.back(), A);

        assertAdd(rb, B, 2, A);
        assertAdd(rb, C, 3, A);
        assertEquals(rb.back(), C);
    }

    public void testBackTailIsZero() {
        PairwiseFloatRingBuffer rb = new PairwiseFloatRingBuffer(5, NULL_FLOAT, Float::min);

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
        try (final PairwiseFloatRingBuffer rb = new PairwiseFloatRingBuffer(3, 0.0f, Float::sum)) {

            // move the head and tail off zero
            for (float i = 0; i < 1000; i++) {
                rb.push(i);
            }
            for (float i = 0; i < 1000; i++) {
                rb.pop();
            }

            for (float i = 0; i < 10_000; i++)
                rb.push(i);

            for (float i = 10_000; i < 1_000_000; i++) {
                rb.push(i);
                assertEquals(i - 10_000 + 1, rb.front(1));
                assertEquals(i - 10_000, rb.pop());
                assertEquals(rb.remaining(), rb.capacity() - rb.size());
            }
        }
    }

    public void testEvaluateMinLargeAmounts() {
        try (final PairwiseFloatRingBuffer rb = new PairwiseFloatRingBuffer(3, Float.MAX_VALUE, Float::min)) {
            for (float i = 0; i < 10_000; i++)
                rb.push(i);

            for (float i = 10_000; i < 1_000_000; i++) {
                rb.push(i);
                assertEquals(i - 10_000 + 1, rb.front(1));
                assertEquals(i - 10_000, rb.evaluate()); // front of queue is min
                assertEquals(i - 10_000, rb.pop());
            }
        }
    }

    public void testEvaluateMaxLargeAmounts() {
        try (final PairwiseFloatRingBuffer rb = new PairwiseFloatRingBuffer(3, Float.MIN_VALUE, Float::max)) {
            for (float i = 0; i < 10_000; i++)
                rb.push(i);

            for (float i = 10_000; i < 1_000_000; i++) {
                rb.push(i);
                assertEquals(i - 10_000 + 1, rb.front(1));
                assertEquals(i, rb.evaluate()); // last value added is max
                assertEquals(i - 10_000, rb.pop());
            }
        }
    }

    public void testEvaluateSumLargeAmounts() {
        try (final PairwiseFloatRingBuffer rb = new PairwiseFloatRingBuffer(3, 0.0f, Float::sum)) {

            float runningSum = 0.0f;

            for (float i = 0; i < 1_000; i++) {
                rb.push(i);
                runningSum += i;
            }

            // stay small enough to avoid floating errors but prove the concept
            for (float i = 1_000; i < 10_000; i++) {
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
        try (final PairwiseFloatRingBuffer rb = new PairwiseFloatRingBuffer(3, -Float.MAX_VALUE, Float::max)) {
            // move the head and tail off zero
            for (float i = 0; i < 500; i++) {
                rb.push(i);
            }
            for (float i = 0; i < 500; i++) {
                rb.pop();
            }

            for (float i = 0; i < 100; i++) {
                rb.push(i);
            }
            assertEquals((float)99, rb.evaluate()); // last value added is max
        }
    }
}
