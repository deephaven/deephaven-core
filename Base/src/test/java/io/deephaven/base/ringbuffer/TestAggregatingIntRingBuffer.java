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

public class TestAggregatingIntRingBuffer extends TestCase {

    private void assertEmpty(AggregatingIntRingBuffer rb) {
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
        final AggregatingIntRingBuffer rb = new AggregatingIntRingBuffer(3, (int) 0, (a, b) -> (int) (a + b));

        // move the head and tail off zero
        for (int i = 0; i < 1000; i++) {
            rb.add((int) i);
        }
        for (int i = 0; i < 1000; i++) {
            rb.remove();
        }

        for (int i = 0; i < 10_000; i++)
            rb.add((int) i);

        for (int i = 10_000; i < 1_000_000; i++) {
            rb.add((int) i);
            assertEquals((int) (i - 10_000 + 1), rb.front(1));
            assertEquals((int) (i - 10_000), rb.remove());
            assertEquals(rb.remaining(), rb.capacity() - rb.size());
        }
    }

    public void testEvaluateMinLargeAmounts() {
        final AggregatingIntRingBuffer rb =
                new AggregatingIntRingBuffer(3, Integer.MAX_VALUE, (a, b) -> (int) Math.min(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((int) i);

        final int maxVal = (int) Math.min(Integer.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((int) i);
            assertEquals((int) (i - 10_000) + 1, rb.front(1));
            assertEquals((int) (i - 10_000), rb.evaluate()); // front of queue is min
            assertEquals((int) (i - 10_000), rb.remove());
        }
    }

    public void testEvaluateMaxLargeAmounts() {
        final AggregatingIntRingBuffer rb =
                new AggregatingIntRingBuffer(3, Integer.MIN_VALUE, (a, b) -> (int) Math.max(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((int) i);

        final int maxVal = (int) Math.min(Integer.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((int) i);
            assertEquals((int) (i - 10_000 + 1), rb.front(1));
            assertEquals((int) i, rb.evaluate()); // last value added is max
            assertEquals((int) (i - 10_000), rb.remove());
        }
    }

    // region non-byte-tests

    public void testEvaluateSumLargeAmounts() {
        final AggregatingIntRingBuffer rb = new AggregatingIntRingBuffer(3, (int) 0, (a, b) -> (int) (a + b));
        int runningSum = (int) 0;

        for (int i = 0; i < 1_000; i++) {
            rb.add(i);
            runningSum += i;
        }

        // stay small enough to avoid inting errors but prove the concept
        for (int i = 1_000; i < 10_000; i++) {
            rb.add(i);
            runningSum += i; // add the current value
            assertEquals((int) (i - 1_000 + 1), rb.front(1));

            assertEquals(runningSum, rb.evaluate());

            assertEquals((int) (i - 1_000), rb.remove());
            runningSum -= i - 1_000; // remove the value 1_0000 ago

            assertEquals(runningSum, rb.evaluate());
        }
    }

    /***
     * Return the sum of 0 to N-1
     */
    private int sum0toN(int n) {
        if (n == (int) 0) {
            return (int) 0; // not negative zero, sigh
        }
        return (int) (n * (n - 1) / 2);
    }

    public void testEvaluationEdgeCase() {
        AggregatingIntRingBuffer rb = new AggregatingIntRingBuffer(512, (int) 0, (a, b) -> (int) (a + b));

        rb.addIdentityValue();
        assertEquals((int) 0, rb.evaluate());
        rb.clear();

        final int PRIME = (int) 97;
        final int PRIME_SUM = sum0toN(PRIME);
        final Random rnd = new Random(0xDEADBEEF);

        // specific test for push or wrap around
        for (int step = 0; step < 100; step++) {
            for (int i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (int i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((int) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((int) 0, rb.evaluate());
        }

        // specific test for push & wrap around
        for (int step = 0; step < 100; step++) {
            for (int i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            for (int i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((int) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((int) 0, rb.evaluate());
        }

        // push random amounts and test
        for (int step = 0; step < 100; step++) {
            final int OFFSET = (int) rnd.nextInt((int) PRIME);

            for (int i = 0; i < OFFSET; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(sum0toN(OFFSET), rb.evaluate());

            for (int i = OFFSET; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (int i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((int) 0, rb.evaluate());
        }

        // pop random amounts and test
        for (int step = 0; step < 100; step++) {
            final int OFFSET = (int) rnd.nextInt((int) PRIME);
            for (int i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (int i = 0; i < OFFSET; i++) {
                rb.removeUnsafe();
            }
            assertEquals(PRIME_SUM - sum0toN(OFFSET), rb.evaluate());

            for (int i = 0; i < (PRIME - OFFSET); i++) {
                rb.removeUnsafe();
            }
            assertEquals((int) 0, rb.evaluate());
        }


        rb = new AggregatingIntRingBuffer(512, (int) 0, (a, b) -> (int) (a + b));
        // need to get the buffer to state where we have clean pushes and a wrapped pop

        // move the pointers close to the end
        for (int i = 0; i < 500; i++) {
            rb.addUnsafe(i);
            rb.removeUnsafe();
        }
        assertEquals((int) 0, rb.evaluate());
        assertEmpty(rb);

        // push past the end
        for (int i = 0; i < 200; i++) {
            rb.addUnsafe(i);
        }
        assertEquals(sum0toN((int) 200), rb.evaluate());

        // one more push to dirty the pushes
        rb.addUnsafe((int) 201);

        // pop past the end
        for (int i = 0; i < 200; i++) {
            rb.removeUnsafe();
        }

        // only thing in the buffer is the final push
        assertEquals((int) 201, rb.evaluate());
    }
    // endregion non-byte-tests

    public void testPushPopUnsafe() {
        final AggregatingIntRingBuffer rb =
                new AggregatingIntRingBuffer(3, (int) -Integer.MAX_VALUE, (a, b) -> (int) Math.max(a, b));
        // move the head and tail off zero
        rb.ensureRemaining(100);
        for (int i = 0; i < 100; i++) {
            rb.addUnsafe(i);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i);
        }

        // do it again with an offset
        rb.ensureRemaining(100);
        for (int i = 0; i < 100; i++) {
            rb.addUnsafe((int) (i + (int) 1000));
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i + (int) 1000);
        }

        for (int i = 0; i < 100; i++) {
            rb.addUnsafe((int) (i + (int) 1000));
        }
        rb.clear();

        for (int i = 0; i < 100; i++) {
            rb.add(i);
        }
        assertEquals((int) 99, rb.evaluate()); // last value added is max
    }

    public void testPopMultiple() {
        final AggregatingIntRingBuffer rb = new AggregatingIntRingBuffer(3, (int) 0, (a, b) -> (int) (a + b));

        for (int step = 0; step < 10; step++) {
            rb.ensureRemaining(100);
            for (int i = 0; i < 100; i++) {
                rb.addUnsafe(i);
            }

            if (step % 2 == 0) {
                rb.evaluate();
            }

            try {
                int[] values = rb.remove(101);
                fail("popping more values than size() should fail");
            } catch (NoSuchElementException x) {
                // expected
            }

            int[] values = rb.remove(100);
            for (int i = 0; i < 100; i++) {
                assertEquals(values[(int) i], i);
            }
            assertEmpty(rb);
        }
    }

    public void testSpecialCaseA() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingIntRingBuffer rb = new AggregatingIntRingBuffer(4, (int) 0, (a, b) -> (int) (a + b));
        rb.add((int) 1);
        assertEquals((int) 1, rb.remove());
        rb.add((int) 2);
        rb.add((int) 3);
        assertEquals((int) 5, rb.evaluate());
    }

    public void testSpecialCaseB() {
        // push the full capacity while wrapped
        final AggregatingIntRingBuffer rb = new AggregatingIntRingBuffer(64, (int) 0, (a, b) -> (int) (a + b));
        rb.add((int) 1);
        assertEquals((int) 1, rb.remove());

        for (int i = 0; i < 64; i++) {
            rb.add((int) 1);
        }
        assertEquals((int) 64, rb.evaluate());
    }

    public void testSpecialCaseC() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingIntRingBuffer rb = new AggregatingIntRingBuffer(16, (int) 0, (a, b) -> (int) (a + b));
        // move pointers to middle of storage
        for (int i = 0; i < 8; i++) {
            rb.add((int) 1);
            rb.remove();
        }
        assertEquals((int) 0, rb.evaluate());

        for (int i = 0; i < 11; i++) {
            rb.add((int) 1);
        }
        rb.remove();
        assertEquals((int) 10, rb.evaluate());
    }
}
