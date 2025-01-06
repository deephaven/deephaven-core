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

public class TestAggregatingFloatRingBuffer extends TestCase {

    private void assertEmpty(AggregatingFloatRingBuffer rb) {
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
        final AggregatingFloatRingBuffer rb = new AggregatingFloatRingBuffer(3, (float) 0, (a, b) -> (float) (a + b));

        // move the head and tail off zero
        for (int i = 0; i < 1000; i++) {
            rb.add((float) i);
        }
        for (int i = 0; i < 1000; i++) {
            rb.remove();
        }

        for (int i = 0; i < 10_000; i++)
            rb.add((float) i);

        for (int i = 10_000; i < 1_000_000; i++) {
            rb.add((float) i);
            assertEquals((float) (i - 10_000 + 1), rb.front(1));
            assertEquals((float) (i - 10_000), rb.remove());
            assertEquals(rb.remaining(), rb.capacity() - rb.size());
        }
    }

    public void testEvaluateMinLargeAmounts() {
        final AggregatingFloatRingBuffer rb =
                new AggregatingFloatRingBuffer(3, Float.MAX_VALUE, (a, b) -> (float) Math.min(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((float) i);

        final int maxVal = (int) Math.min(Float.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((float) i);
            assertEquals((float) (i - 10_000) + 1, rb.front(1));
            assertEquals((float) (i - 10_000), rb.evaluate()); // front of queue is min
            assertEquals((float) (i - 10_000), rb.remove());
        }
    }

    public void testEvaluateMaxLargeAmounts() {
        final AggregatingFloatRingBuffer rb =
                new AggregatingFloatRingBuffer(3, Float.MIN_VALUE, (a, b) -> (float) Math.max(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((float) i);

        final int maxVal = (int) Math.min(Float.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((float) i);
            assertEquals((float) (i - 10_000 + 1), rb.front(1));
            assertEquals((float) i, rb.evaluate()); // last value added is max
            assertEquals((float) (i - 10_000), rb.remove());
        }
    }

    // region non-byte-tests

    public void testEvaluateSumLargeAmounts() {
        final AggregatingFloatRingBuffer rb = new AggregatingFloatRingBuffer(3, (float) 0, (a, b) -> (float) (a + b));
        float runningSum = (float) 0;

        for (float i = 0; i < 1_000; i++) {
            rb.add(i);
            runningSum += i;
        }

        // stay small enough to avoid floating errors but prove the concept
        for (float i = 1_000; i < 10_000; i++) {
            rb.add(i);
            runningSum += i; // add the current value
            assertEquals((float) (i - 1_000 + 1), rb.front(1));

            assertEquals(runningSum, rb.evaluate());

            assertEquals((float) (i - 1_000), rb.remove());
            runningSum -= i - 1_000; // remove the value 1_0000 ago

            assertEquals(runningSum, rb.evaluate());
        }
    }

    /***
     * Return the sum of 0 to N-1
     */
    private float sum0toN(float n) {
        if (n == (float) 0) {
            return (float) 0; // not negative zero, sigh
        }
        return (float) (n * (n - 1) / 2);
    }

    public void testEvaluationEdgeCase() {
        AggregatingFloatRingBuffer rb = new AggregatingFloatRingBuffer(512, (float) 0, (a, b) -> (float) (a + b));

        rb.addIdentityValue();
        assertEquals((float) 0, rb.evaluate());
        rb.clear();

        final float PRIME = (float) 97;
        final float PRIME_SUM = sum0toN(PRIME);
        final Random rnd = new Random(0xDEADBEEF);

        // specific test for push or wrap around
        for (int step = 0; step < 100; step++) {
            for (float i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (float i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((float) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((float) 0, rb.evaluate());
        }

        // specific test for push & wrap around
        for (int step = 0; step < 100; step++) {
            for (float i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            for (float i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((float) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((float) 0, rb.evaluate());
        }

        // push random amounts and test
        for (int step = 0; step < 100; step++) {
            final float OFFSET = (float) rnd.nextInt((int) PRIME);

            for (float i = 0; i < OFFSET; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(sum0toN(OFFSET), rb.evaluate());

            for (float i = OFFSET; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (float i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((float) 0, rb.evaluate());
        }

        // pop random amounts and test
        for (int step = 0; step < 100; step++) {
            final float OFFSET = (float) rnd.nextInt((int) PRIME);
            for (float i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (float i = 0; i < OFFSET; i++) {
                rb.removeUnsafe();
            }
            assertEquals(PRIME_SUM - sum0toN(OFFSET), rb.evaluate());

            for (float i = 0; i < (PRIME - OFFSET); i++) {
                rb.removeUnsafe();
            }
            assertEquals((float) 0, rb.evaluate());
        }


        rb = new AggregatingFloatRingBuffer(512, (float) 0, (a, b) -> (float) (a + b));
        // need to get the buffer to state where we have clean pushes and a wrapped pop

        // move the pointers close to the end
        for (float i = 0; i < 500; i++) {
            rb.addUnsafe(i);
            rb.removeUnsafe();
        }
        assertEquals((float) 0, rb.evaluate());
        assertEmpty(rb);

        // push past the end
        for (float i = 0; i < 200; i++) {
            rb.addUnsafe(i);
        }
        assertEquals(sum0toN((float) 200), rb.evaluate());

        // one more push to dirty the pushes
        rb.addUnsafe((float) 201);

        // pop past the end
        for (float i = 0; i < 200; i++) {
            rb.removeUnsafe();
        }

        // only thing in the buffer is the final push
        assertEquals((float) 201, rb.evaluate());
    }
    // endregion non-byte-tests

    public void testPushPopUnsafe() {
        final AggregatingFloatRingBuffer rb =
                new AggregatingFloatRingBuffer(3, (float) -Float.MAX_VALUE, (a, b) -> (float) Math.max(a, b));
        // move the head and tail off zero
        rb.ensureRemaining(100);
        for (float i = 0; i < 100; i++) {
            rb.addUnsafe(i);
        }
        for (float i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i);
        }

        // do it again with an offset
        rb.ensureRemaining(100);
        for (float i = 0; i < 100; i++) {
            rb.addUnsafe((float) (i + (float) 1000));
        }
        for (float i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i + (float) 1000);
        }

        for (float i = 0; i < 100; i++) {
            rb.addUnsafe((float) (i + (float) 1000));
        }
        rb.clear();

        for (float i = 0; i < 100; i++) {
            rb.add(i);
        }
        assertEquals((float) 99, rb.evaluate()); // last value added is max
    }

    public void testPopMultiple() {
        final AggregatingFloatRingBuffer rb = new AggregatingFloatRingBuffer(3, (float) 0, (a, b) -> (float) (a + b));

        for (int step = 0; step < 10; step++) {
            rb.ensureRemaining(100);
            for (float i = 0; i < 100; i++) {
                rb.addUnsafe(i);
            }

            if (step % 2 == 0) {
                rb.evaluate();
            }

            try {
                float[] values = rb.remove(101);
                fail("popping more values than size() should fail");
            } catch (NoSuchElementException x) {
                // expected
            }

            float[] values = rb.remove(100);
            for (float i = 0; i < 100; i++) {
                assertEquals(values[(int) i], i);
            }
            assertEmpty(rb);
        }
    }

    public void testSpecialCaseA() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingFloatRingBuffer rb = new AggregatingFloatRingBuffer(4, (float) 0, (a, b) -> (float) (a + b));
        rb.add((float) 1);
        assertEquals((float) 1, rb.remove());
        rb.add((float) 2);
        rb.add((float) 3);
        assertEquals((float) 5, rb.evaluate());
    }

    public void testSpecialCaseB() {
        // push the full capacity while wrapped
        final AggregatingFloatRingBuffer rb = new AggregatingFloatRingBuffer(64, (float) 0, (a, b) -> (float) (a + b));
        rb.add((float) 1);
        assertEquals((float) 1, rb.remove());

        for (int i = 0; i < 64; i++) {
            rb.add((float) 1);
        }
        assertEquals((float) 64, rb.evaluate());
    }

    public void testSpecialCaseC() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingFloatRingBuffer rb = new AggregatingFloatRingBuffer(16, (float) 0, (a, b) -> (float) (a + b));
        // move pointers to middle of storage
        for (int i = 0; i < 8; i++) {
            rb.add((float) 1);
            rb.remove();
        }
        assertEquals((float) 0, rb.evaluate());

        for (int i = 0; i < 11; i++) {
            rb.add((float) 1);
        }
        rb.remove();
        assertEquals((float) 10, rb.evaluate());
    }
}
