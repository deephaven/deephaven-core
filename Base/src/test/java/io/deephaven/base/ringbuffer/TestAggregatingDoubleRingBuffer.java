//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestAggregatingCharRingBuffer and run "./gradlew replicateRingBuffers" to regenerate
//
// @formatter:off
package io.deephaven.base.ringbuffer;

import junit.framework.TestCase;

import java.util.NoSuchElementException;
import java.util.Random;

public class TestAggregatingDoubleRingBuffer extends TestCase {

    private void assertEmpty(AggregatingDoubleRingBuffer rb) {
        assertTrue(rb.isEmpty());
        assertEquals(0, rb.size());

        try {
            rb.remove();
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

    public void testLargeAmounts() {
        final AggregatingDoubleRingBuffer rb = new AggregatingDoubleRingBuffer(3, (double) 0, (a, b) -> (double) (a + b));

        // move the head and tail off zero
        for (int i = 0; i < 1000; i++) {
            rb.add((double) i);
        }
        for (int i = 0; i < 1000; i++) {
            rb.remove();
        }

        for (int i = 0; i < 10_000; i++)
            rb.add((double) i);

        for (int i = 10_000; i < 1_000_000; i++) {
            rb.add((double) i);
            assertEquals((double) (i - 10_000 + 1), rb.front(1));
            assertEquals((double) (i - 10_000), rb.remove());
            assertEquals(rb.remaining(), rb.capacity() - rb.size());
        }
    }

    public void testEvaluateMinLargeAmounts() {
        final AggregatingDoubleRingBuffer rb =
                new AggregatingDoubleRingBuffer(3, Double.MAX_VALUE, (a, b) -> (double) Math.min(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((double) i);

        final int maxVal = (int) Math.min(Double.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((double) i);
            assertEquals((double) (i - 10_000) + 1, rb.front(1));
            assertEquals((double) (i - 10_000), rb.evaluate()); // front of queue is min
            assertEquals((double) (i - 10_000), rb.remove());
        }
    }

    public void testEvaluateMaxLargeAmounts() {
        final AggregatingDoubleRingBuffer rb =
                new AggregatingDoubleRingBuffer(3, Double.MIN_VALUE, (a, b) -> (double) Math.max(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((double) i);

        final int maxVal = (int) Math.min(Double.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((double) i);
            assertEquals((double) (i - 10_000 + 1), rb.front(1));
            assertEquals((double) i, rb.evaluate()); // last value added is max
            assertEquals((double) (i - 10_000), rb.remove());
        }
    }

    // region non-byte-tests

    public void testEvaluateSumLargeAmounts() {
        final AggregatingDoubleRingBuffer rb = new AggregatingDoubleRingBuffer(3, (double) 0, (a, b) -> (double) (a + b));
        double runningSum = (double) 0;

        for (double i = 0; i < 1_000; i++) {
            rb.add(i);
            runningSum += i;
        }

        // stay small enough to avoid doubleing errors but prove the concept
        for (double i = 1_000; i < 10_000; i++) {
            rb.add(i);
            runningSum += i; // add the current value
            assertEquals((double) (i - 1_000 + 1), rb.front(1));

            assertEquals(runningSum, rb.evaluate());

            assertEquals((double) (i - 1_000), rb.remove());
            runningSum -= i - 1_000; // remove the value 1_0000 ago

            assertEquals(runningSum, rb.evaluate());
        }
    }

    /***
     * Return the sum of 0 to N-1
     */
    private double sum0toN(double n) {
        if (n == (double) 0) {
            return (double) 0; // not negative zero, sigh
        }
        return (double) (n * (n - 1) / 2);
    }

    public void testEvaluationEdgeCase() {
        AggregatingDoubleRingBuffer rb = new AggregatingDoubleRingBuffer(512, (double) 0, (a, b) -> (double) (a + b));

        rb.addIdentityValue();
        assertEquals((double) 0, rb.evaluate());
        rb.clear();

        final double PRIME = (double) 97;
        final double PRIME_SUM = sum0toN(PRIME);
        final Random rnd = new Random(0xDEADBEEF);

        // specific test for push or wrap around
        for (int step = 0; step < 100; step++) {
            for (double i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (double i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((double) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((double) 0, rb.evaluate());
        }

        // specific test for push & wrap around
        for (int step = 0; step < 100; step++) {
            for (double i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            for (double i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((double) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((double) 0, rb.evaluate());
        }

        // push random amounts and test
        for (int step = 0; step < 100; step++) {
            final double OFFSET = (double) rnd.nextInt((int) PRIME);

            for (double i = 0; i < OFFSET; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(sum0toN(OFFSET), rb.evaluate());

            for (double i = OFFSET; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (double i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((double) 0, rb.evaluate());
        }

        // pop random amounts and test
        for (int step = 0; step < 100; step++) {
            final double OFFSET = (double) rnd.nextInt((int) PRIME);
            for (double i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (double i = 0; i < OFFSET; i++) {
                rb.removeUnsafe();
            }
            assertEquals(PRIME_SUM - sum0toN(OFFSET), rb.evaluate());

            for (double i = 0; i < (PRIME - OFFSET); i++) {
                rb.removeUnsafe();
            }
            assertEquals((double) 0, rb.evaluate());
        }


        rb = new AggregatingDoubleRingBuffer(512, (double) 0, (a, b) -> (double) (a + b));
        // need to get the buffer to state where we have clean pushes and a wrapped pop

        // move the pointers close to the end
        for (double i = 0; i < 500; i++) {
            rb.addUnsafe(i);
            rb.removeUnsafe();
        }
        assertEquals((double) 0, rb.evaluate());
        assertEmpty(rb);

        // push past the end
        for (double i = 0; i < 200; i++) {
            rb.addUnsafe(i);
        }
        assertEquals(sum0toN((double) 200), rb.evaluate());

        // one more push to dirty the pushes
        rb.addUnsafe((double) 201);

        // pop past the end
        for (double i = 0; i < 200; i++) {
            rb.removeUnsafe();
        }

        // only thing in the buffer is the final push
        assertEquals((double) 201, rb.evaluate());
    }
    // endregion non-byte-tests

    public void testPushPopUnsafe() {
        final AggregatingDoubleRingBuffer rb =
                new AggregatingDoubleRingBuffer(3, (double) -Double.MAX_VALUE, (a, b) -> (double) Math.max(a, b));
        // move the head and tail off zero
        rb.ensureRemaining(100);
        for (double i = 0; i < 100; i++) {
            rb.addUnsafe(i);
        }
        for (double i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i);
        }

        // do it again with an offset
        rb.ensureRemaining(100);
        for (double i = 0; i < 100; i++) {
            rb.addUnsafe((double) (i + (double) 1000));
        }
        for (double i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i + (double) 1000);
        }

        for (double i = 0; i < 100; i++) {
            rb.addUnsafe((double) (i + (double) 1000));
        }
        rb.clear();

        for (double i = 0; i < 100; i++) {
            rb.add(i);
        }
        assertEquals((double) 99, rb.evaluate()); // last value added is max
    }

    public void testPopMultiple() {
        final AggregatingDoubleRingBuffer rb = new AggregatingDoubleRingBuffer(3, (double) 0, (a, b) -> (double) (a + b));

        for (int step = 0; step < 10; step++) {
            rb.ensureRemaining(100);
            for (double i = 0; i < 100; i++) {
                rb.addUnsafe(i);
            }

            if (step % 2 == 0) {
                rb.evaluate();
            }

            try {
                double[] values = rb.remove(101);
                fail("popping more values than size() should fail");
            } catch (NoSuchElementException x) {
                // expected
            }

            double[] values = rb.remove(100);
            for (double i = 0; i < 100; i++) {
                assertEquals(values[(int) i], i);
            }
            assertEmpty(rb);
        }
    }

    public void testSpecialCaseA() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingDoubleRingBuffer rb = new AggregatingDoubleRingBuffer(4, (double) 0, (a, b) -> (double) (a + b));
        rb.add((double) 1);
        assertEquals((double) 1, rb.remove());
        rb.add((double) 2);
        rb.add((double) 3);
        assertEquals((double) 5, rb.evaluate());
    }

    public void testSpecialCaseB() {
        // push the full capacity while wrapped
        final AggregatingDoubleRingBuffer rb = new AggregatingDoubleRingBuffer(64, (double) 0, (a, b) -> (double) (a + b));
        rb.add((double) 1);
        assertEquals((double) 1, rb.remove());

        for (int i = 0; i < 64; i++) {
            rb.add((double) 1);
        }
        assertEquals((double) 64, rb.evaluate());
    }

    public void testSpecialCaseC() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingDoubleRingBuffer rb = new AggregatingDoubleRingBuffer(16, (double) 0, (a, b) -> (double) (a + b));
        // move pointers to middle of storage
        for (int i = 0; i < 8; i++) {
            rb.add((double) 1);
            rb.remove();
        }
        assertEquals((double) 0, rb.evaluate());

        for (int i = 0; i < 11; i++) {
            rb.add((double) 1);
        }
        rb.remove();
        assertEquals((double) 10, rb.evaluate());
    }
}
