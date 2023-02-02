/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PairwiseFloatRingBufferTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;
import org.junit.experimental.categories.Category;

import java.util.NoSuchElementException;
import java.util.Random;

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

        assertEquals(expectedHead, rb.peek(Long.MIN_VALUE));
        try {
            assertEquals(expectedHead, rb.element());
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
            assertEquals(expectedHead, rb.pop());
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

        try (PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(3, NULL_DOUBLE, Double::min)) {

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
            assertEquals(rb.back(), C);
            assertEquals(rb.peekBack(NULL_DOUBLE), C);

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
    }

    public void testGrowSimple() {
        try (PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(5, NULL_DOUBLE, Double::min)) {

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
    }

    public void testGrowComplex() {
        try (PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(5, NULL_DOUBLE, Double::min)) {

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
    }

    public void testWhenEmpty() {
        try (PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(5, NULL_DOUBLE, Double::min)) {
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
            assertEquals(rb.poll((double) -1), (double) -1);
            assertEquals(rb.peek((double) -1), (double) -1);
            assertEquals(rb.peekBack((double) -1), (double) -1);
        }
    }

    public void testBack() {
        try (PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(5, NULL_DOUBLE, Double::min)) {

            assertAdd(rb, A, 1, A);
            assertEquals(rb.back(), A);

            assertAdd(rb, B, 2, A);
            assertAdd(rb, C, 3, A);
            assertEquals(rb.back(), C);
        }
    }

    public void testBackTailIsZero() {
        try (PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(5, NULL_DOUBLE, Double::min)) {

            assertAdd(rb, A, 1, A);
            assertAdd(rb, B, 2, A);
            assertAdd(rb, C, 3, A);
            assertAdd(rb, D, 4, A);
            assertAdd(rb, E, 5, A);

            assertRemove(rb, 5, A);
            assertAdd(rb, F, 5, B);
            assertEquals(rb.back(), F);
        }
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

    /***
     * Return the sum of 0 to N-1
     */
    private double sum0toN(double n) {
        if (n == (double) 0) {
            return (double) 0; // not negative zero, sigh
        }
        return (n * (n - 1) / (double) 2);
    }

    public void testEvaluationEdgeCase() {
        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(512, (double) 0, Double::sum)) {

            final int PRIME = 97;
            final double PRIME_SUM = sum0toN(PRIME);
            final Random rnd = new Random(0xDEADBEEF);

            // specific test for push or wrap araound
            for (int step = 0; step < 100; step++) {
                for (double i = 0; i < PRIME; i++) {
                    rb.pushUnsafe(i);
                }
                assertEquals(PRIME_SUM, rb.evaluate());

                for (double i = 0; i < PRIME; i++) {
                    rb.popUnsafe();
                }
                assertEquals((double) 0, rb.evaluate());
                // not a copy/paste error, call this twice
                assertEquals((double) 0, rb.evaluate());
            }

            // specific test for push & wrap araound
            for (int step = 0; step < 100; step++) {
                for (double i = 0; i < PRIME; i++) {
                    rb.pushUnsafe(i);
                }
                for (double i = 0; i < PRIME; i++) {
                    rb.popUnsafe();
                }
                assertEquals((double) 0, rb.evaluate());
                // not a copy/paste error, call this twice
                assertEquals((double) 0, rb.evaluate());
            }

            // push random amounts and test
            for (int step = 0; step < 100; step++) {
                final int OFFSET = rnd.nextInt(PRIME);

                for (double i = 0; i < OFFSET; i++) {
                    rb.pushUnsafe(i);
                }
                assertEquals(sum0toN(OFFSET), rb.evaluate());

                for (double i = OFFSET; i < PRIME; i++) {
                    rb.pushUnsafe(i);
                }
                assertEquals(PRIME_SUM, rb.evaluate());

                for (double i = 0; i < PRIME; i++) {
                    rb.popUnsafe();
                }
                assertEquals((double) 0, rb.evaluate());
            }

            // pop random amounts and test
            for (int step = 0; step < 100; step++) {
                final int OFFSET = rnd.nextInt(PRIME);
                for (double i = 0; i < PRIME; i++) {
                    rb.pushUnsafe(i);
                }
                assertEquals(PRIME_SUM, rb.evaluate());

                for (double i = 0; i < OFFSET; i++) {
                    rb.popUnsafe();
                }
                assertEquals(PRIME_SUM - sum0toN(OFFSET), rb.evaluate());

                for (double i = 0; i < (PRIME - OFFSET); i++) {
                    rb.popUnsafe();
                }
                assertEquals((double) 0, rb.evaluate());
            }
        }

        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(512, (double) 0, Double::sum)) {
            // need to get the buffer to state where we have clean pushes and a wrapped pop

            // move the pointers close to the end
            for (double i = 0; i < 500; i++) {
                rb.pushUnsafe(i);
                rb.popUnsafe();
            }
            assertEquals((double) 0, rb.evaluate());
            assertEmpty(rb);

            // push past the end
            for (double i = 0; i < 200; i++) {
                rb.pushUnsafe(i);
            }
            assertEquals(sum0toN(200), rb.evaluate());

            // one more push to dirty the pushes
            rb.pushUnsafe(201);

            // pop past the end
            for (double i = 0; i < 200; i++) {
                rb.popUnsafe();
            }

            // only thing in the buffer is the final push
            assertEquals((double) 201, rb.evaluate());
        }

        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(512, (double) 0, Double::sum)) {
            // cue some hacky stuff to improve code coverage
            for (double i = 0; i < 100; i++) {
                rb.pushUnsafe(i);
            }

            final double sum = sum0toN(100);

            // evaluateTree() doesn't reset the dirty bits so these calls can follow each other

            // no consecutive ranges
            assertEquals(sum, rb.evaluateTree(512, 611, 712, 811, 912, 1023));
            // first two ranges are consecutive
            assertEquals(sum, rb.evaluateTree(512, 611, 612, 811, 912, 1023));
            // second two ranges are consecutive
            assertEquals(sum, rb.evaluateTree(512, 611, 712, 811, 812, 1023));
            // all ranges are consecutive
            assertEquals(sum, rb.evaluateTree(512, 611, 612, 811, 812, 1023));
        }
    }

    public void testPushPopUnsafe() {
        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(3, -Double.MAX_VALUE, Double::max)) {
            // move the head and tail off zero
            rb.ensureRemaining(500);
            for (double i = 0; i < 500; i++) {
                rb.pushUnsafe(i);
            }
            for (double i = 0; i < 500; i++) {
                assertEquals(rb.popUnsafe(), i);
            }

            // do it again with an offset
            rb.ensureRemaining(500);
            for (double i = 0; i < 500; i++) {
                rb.pushUnsafe(i + (double) 1000);
            }
            for (double i = 0; i < 500; i++) {
                assertEquals(rb.popUnsafe(), i + (double) 1000);
            }

            for (double i = 0; i < 500; i++) {
                rb.pushUnsafe(i + (double) 1000);
            }
            rb.clear();

            for (double i = 0; i < 100; i++) {
                rb.push(i);
            }
            assertEquals((double) 99, rb.evaluate()); // last value added is max
        }
    }

    public void testPopMultiple() {
        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(3, (double) 0, Double::sum)) {

            for (int step = 0; step < 10; step++) {
                rb.ensureRemaining(500);
                for (double i = 0; i < 500; i++) {
                    rb.pushUnsafe(i);
                }

                if (step % 2 == 0) {
                    rb.evaluate();
                }

                try {
                    double[] values = rb.pop(501);
                    fail("popping more values than size() should fail");
                } catch (NoSuchElementException x) {
                    // expected
                }

                double[] values = rb.pop(500);
                for (double i = 0; i < 500; i++) {
                    assertEquals(values[(int) i], i);
                }
                assertEmpty(rb);
            }
        }
    }

    public void testRangesCollapse() {
        // overlapping
        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(0, 2, 1, 3));
        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(1, 3, 0, 2));

        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(0, 10, 1, 3));
        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(1, 3, 0, 10));

        // fully contained
        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(0, 3, 1, 2));
        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(1, 2, 0, 3));

        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(1, 2, 1, 1));
        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(1, 1, 1, 2));

        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(1, 2, 2, 2));
        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(2, 2, 1, 2));

        // consecutive
        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(0, 1, 2, 3));
        assertTrue(PairwiseDoubleRingBuffer.rangesCollapse(2, 3, 0, 1));

        // non-overlapping, non consecutive
        assertFalse(PairwiseDoubleRingBuffer.rangesCollapse(0, 1, 3, 4));
        assertFalse(PairwiseDoubleRingBuffer.rangesCollapse(3, 4, 0, 1));

        assertFalse(PairwiseDoubleRingBuffer.rangesCollapse(0, 1, 9, 10));
        assertFalse(PairwiseDoubleRingBuffer.rangesCollapse(9, 10, 0, 1));
    }

    public void testSpecialCaseA() {
        // overlapping push and pop ranges with popTail < pushTail
        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(4, (double) 0, Double::sum)) {
            rb.push((double)1);
            assertEquals((double)1, rb.pop());
            rb.push((double)2);
            rb.push((double)3);
            assertEquals((double)5, rb.evaluate());
        }
    }

    public void testSpecialCaseB() {
        // push the full capacity while wrapped
        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(64, (double) 0, Double::sum)) {
            rb.push((double)1);
            assertEquals((double)1, rb.pop());

            for (int i = 0; i < 64; i++) {
                rb.push((double)1);
            }
            assertEquals((double)64, rb.evaluate());
        }
    }

    public void testSpecialCaseC() {
        // overlapping push and pop ranges with popTail < pushTail
        try (final PairwiseDoubleRingBuffer rb = new PairwiseDoubleRingBuffer(16, (double) 0, Double::sum)) {
            // move pointers to middle of storage
            for (int i = 0; i < 8; i++) {
                rb.push((double)1);
                rb.pop();
            }
            assertEquals((double)0, rb.evaluate());

            for (int i = 0; i < 11; i++) {
                rb.push((double)1);
            }
            rb.pop();
            assertEquals((double)10, rb.evaluate());
        }
    }
}
