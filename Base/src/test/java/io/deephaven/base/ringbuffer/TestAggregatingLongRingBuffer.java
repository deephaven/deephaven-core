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

public class TestAggregatingLongRingBuffer extends TestCase {

    private void assertEmpty(AggregatingLongRingBuffer rb) {
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
        final AggregatingLongRingBuffer rb = new AggregatingLongRingBuffer(3, (long) 0, (a, b) -> (long) (a + b));

        // move the head and tail off zero
        for (int i = 0; i < 1000; i++) {
            rb.add((long) i);
        }
        for (int i = 0; i < 1000; i++) {
            rb.remove();
        }

        for (int i = 0; i < 10_000; i++)
            rb.add((long) i);

        for (int i = 10_000; i < 1_000_000; i++) {
            rb.add((long) i);
            assertEquals((long) (i - 10_000 + 1), rb.front(1));
            assertEquals((long) (i - 10_000), rb.remove());
            assertEquals(rb.remaining(), rb.capacity() - rb.size());
        }
    }

    public void testEvaluateMinLargeAmounts() {
        final AggregatingLongRingBuffer rb =
                new AggregatingLongRingBuffer(3, Long.MAX_VALUE, (a, b) -> (long) Math.min(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((long) i);

        final int maxVal = (int) Math.min(Long.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((long) i);
            assertEquals((long) (i - 10_000) + 1, rb.front(1));
            assertEquals((long) (i - 10_000), rb.evaluate()); // front of queue is min
            assertEquals((long) (i - 10_000), rb.remove());
        }
    }

    public void testEvaluateMaxLargeAmounts() {
        final AggregatingLongRingBuffer rb =
                new AggregatingLongRingBuffer(3, Long.MIN_VALUE, (a, b) -> (long) Math.max(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((long) i);

        final int maxVal = (int) Math.min(Long.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((long) i);
            assertEquals((long) (i - 10_000 + 1), rb.front(1));
            assertEquals((long) i, rb.evaluate()); // last value added is max
            assertEquals((long) (i - 10_000), rb.remove());
        }
    }

    // region non-byte-tests

    public void testEvaluateSumLargeAmounts() {
        final AggregatingLongRingBuffer rb = new AggregatingLongRingBuffer(3, (long) 0, (a, b) -> (long) (a + b));
        long runningSum = (long) 0;

        for (long i = 0; i < 1_000; i++) {
            rb.add(i);
            runningSum += i;
        }

        // stay small enough to avoid longing errors but prove the concept
        for (long i = 1_000; i < 10_000; i++) {
            rb.add(i);
            runningSum += i; // add the current value
            assertEquals((long) (i - 1_000 + 1), rb.front(1));

            assertEquals(runningSum, rb.evaluate());

            assertEquals((long) (i - 1_000), rb.remove());
            runningSum -= i - 1_000; // remove the value 1_0000 ago

            assertEquals(runningSum, rb.evaluate());
        }
    }

    /***
     * Return the sum of 0 to N-1
     */
    private long sum0toN(long n) {
        if (n == (long) 0) {
            return (long) 0; // not negative zero, sigh
        }
        return (long) (n * (n - 1) / 2);
    }

    public void testEvaluationEdgeCase() {
        AggregatingLongRingBuffer rb = new AggregatingLongRingBuffer(512, (long) 0, (a, b) -> (long) (a + b));

        rb.addIdentityValue();
        assertEquals((long) 0, rb.evaluate());
        rb.clear();

        final long PRIME = (long) 97;
        final long PRIME_SUM = sum0toN(PRIME);
        final Random rnd = new Random(0xDEADBEEF);

        // specific test for push or wrap around
        for (int step = 0; step < 100; step++) {
            for (long i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (long i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((long) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((long) 0, rb.evaluate());
        }

        // specific test for push & wrap around
        for (int step = 0; step < 100; step++) {
            for (long i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            for (long i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((long) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((long) 0, rb.evaluate());
        }

        // push random amounts and test
        for (int step = 0; step < 100; step++) {
            final long OFFSET = (long) rnd.nextInt((int) PRIME);

            for (long i = 0; i < OFFSET; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(sum0toN(OFFSET), rb.evaluate());

            for (long i = OFFSET; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (long i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((long) 0, rb.evaluate());
        }

        // pop random amounts and test
        for (int step = 0; step < 100; step++) {
            final long OFFSET = (long) rnd.nextInt((int) PRIME);
            for (long i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (long i = 0; i < OFFSET; i++) {
                rb.removeUnsafe();
            }
            assertEquals(PRIME_SUM - sum0toN(OFFSET), rb.evaluate());

            for (long i = 0; i < (PRIME - OFFSET); i++) {
                rb.removeUnsafe();
            }
            assertEquals((long) 0, rb.evaluate());
        }


        rb = new AggregatingLongRingBuffer(512, (long) 0, (a, b) -> (long) (a + b));
        // need to get the buffer to state where we have clean pushes and a wrapped pop

        // move the pointers close to the end
        for (long i = 0; i < 500; i++) {
            rb.addUnsafe(i);
            rb.removeUnsafe();
        }
        assertEquals((long) 0, rb.evaluate());
        assertEmpty(rb);

        // push past the end
        for (long i = 0; i < 200; i++) {
            rb.addUnsafe(i);
        }
        assertEquals(sum0toN((long) 200), rb.evaluate());

        // one more push to dirty the pushes
        rb.addUnsafe((long) 201);

        // pop past the end
        for (long i = 0; i < 200; i++) {
            rb.removeUnsafe();
        }

        // only thing in the buffer is the final push
        assertEquals((long) 201, rb.evaluate());
    }
    // endregion non-byte-tests

    public void testPushPopUnsafe() {
        final AggregatingLongRingBuffer rb =
                new AggregatingLongRingBuffer(3, (long) -Long.MAX_VALUE, (a, b) -> (long) Math.max(a, b));
        // move the head and tail off zero
        rb.ensureRemaining(100);
        for (long i = 0; i < 100; i++) {
            rb.addUnsafe(i);
        }
        for (long i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i);
        }

        // do it again with an offset
        rb.ensureRemaining(100);
        for (long i = 0; i < 100; i++) {
            rb.addUnsafe((long) (i + (long) 1000));
        }
        for (long i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i + (long) 1000);
        }

        for (long i = 0; i < 100; i++) {
            rb.addUnsafe((long) (i + (long) 1000));
        }
        rb.clear();

        for (long i = 0; i < 100; i++) {
            rb.add(i);
        }
        assertEquals((long) 99, rb.evaluate()); // last value added is max
    }

    public void testPopMultiple() {
        final AggregatingLongRingBuffer rb = new AggregatingLongRingBuffer(3, (long) 0, (a, b) -> (long) (a + b));

        for (int step = 0; step < 10; step++) {
            rb.ensureRemaining(100);
            for (long i = 0; i < 100; i++) {
                rb.addUnsafe(i);
            }

            if (step % 2 == 0) {
                rb.evaluate();
            }

            try {
                long[] values = rb.remove(101);
                fail("popping more values than size() should fail");
            } catch (NoSuchElementException x) {
                // expected
            }

            long[] values = rb.remove(100);
            for (long i = 0; i < 100; i++) {
                assertEquals(values[(int) i], i);
            }
            assertEmpty(rb);
        }
    }

    public void testSpecialCaseA() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingLongRingBuffer rb = new AggregatingLongRingBuffer(4, (long) 0, (a, b) -> (long) (a + b));
        rb.add((long) 1);
        assertEquals((long) 1, rb.remove());
        rb.add((long) 2);
        rb.add((long) 3);
        assertEquals((long) 5, rb.evaluate());
    }

    public void testSpecialCaseB() {
        // push the full capacity while wrapped
        final AggregatingLongRingBuffer rb = new AggregatingLongRingBuffer(64, (long) 0, (a, b) -> (long) (a + b));
        rb.add((long) 1);
        assertEquals((long) 1, rb.remove());

        for (int i = 0; i < 64; i++) {
            rb.add((long) 1);
        }
        assertEquals((long) 64, rb.evaluate());
    }

    public void testSpecialCaseC() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingLongRingBuffer rb = new AggregatingLongRingBuffer(16, (long) 0, (a, b) -> (long) (a + b));
        // move pointers to middle of storage
        for (int i = 0; i < 8; i++) {
            rb.add((long) 1);
            rb.remove();
        }
        assertEquals((long) 0, rb.evaluate());

        for (int i = 0; i < 11; i++) {
            rb.add((long) 1);
        }
        rb.remove();
        assertEquals((long) 10, rb.evaluate());
    }
}
