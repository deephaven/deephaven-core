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

public class TestAggregatingShortRingBuffer extends TestCase {

    private void assertEmpty(AggregatingShortRingBuffer rb) {
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
        final AggregatingShortRingBuffer rb = new AggregatingShortRingBuffer(3, (short) 0, (a, b) -> (short) (a + b));

        // move the head and tail off zero
        for (int i = 0; i < 1000; i++) {
            rb.add((short) i);
        }
        for (int i = 0; i < 1000; i++) {
            rb.remove();
        }

        for (int i = 0; i < 10_000; i++)
            rb.add((short) i);

        for (int i = 10_000; i < 1_000_000; i++) {
            rb.add((short) i);
            assertEquals((short) (i - 10_000 + 1), rb.front(1));
            assertEquals((short) (i - 10_000), rb.remove());
            assertEquals(rb.remaining(), rb.capacity() - rb.size());
        }
    }

    public void testEvaluateMinLargeAmounts() {
        final AggregatingShortRingBuffer rb =
                new AggregatingShortRingBuffer(3, Short.MAX_VALUE, (a, b) -> (short) Math.min(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((short) i);

        final int maxVal = (int) Math.min(Short.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((short) i);
            assertEquals((short) (i - 10_000) + 1, rb.front(1));
            assertEquals((short) (i - 10_000), rb.evaluate()); // front of queue is min
            assertEquals((short) (i - 10_000), rb.remove());
        }
    }

    public void testEvaluateMaxLargeAmounts() {
        final AggregatingShortRingBuffer rb =
                new AggregatingShortRingBuffer(3, Short.MIN_VALUE, (a, b) -> (short) Math.max(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((short) i);

        final int maxVal = (int) Math.min(Short.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((short) i);
            assertEquals((short) (i - 10_000 + 1), rb.front(1));
            assertEquals((short) i, rb.evaluate()); // last value added is max
            assertEquals((short) (i - 10_000), rb.remove());
        }
    }

    // region non-byte-tests

    public void testEvaluateSumLargeAmounts() {
        final AggregatingShortRingBuffer rb = new AggregatingShortRingBuffer(3, (short) 0, (a, b) -> (short) (a + b));
        short runningSum = (short) 0;

        for (short i = 0; i < 1_000; i++) {
            rb.add(i);
            runningSum += i;
        }

        // stay small enough to avoid shorting errors but prove the concept
        for (short i = 1_000; i < 10_000; i++) {
            rb.add(i);
            runningSum += i; // add the current value
            assertEquals((short) (i - 1_000 + 1), rb.front(1));

            assertEquals(runningSum, rb.evaluate());

            assertEquals((short) (i - 1_000), rb.remove());
            runningSum -= i - 1_000; // remove the value 1_0000 ago

            assertEquals(runningSum, rb.evaluate());
        }
    }

    /***
     * Return the sum of 0 to N-1
     */
    private short sum0toN(short n) {
        if (n == (short) 0) {
            return (short) 0; // not negative zero, sigh
        }
        return (short) (n * (n - 1) / 2);
    }

    public void testEvaluationEdgeCase() {
        AggregatingShortRingBuffer rb = new AggregatingShortRingBuffer(512, (short) 0, (a, b) -> (short) (a + b));

        rb.addIdentityValue();
        assertEquals((short) 0, rb.evaluate());
        rb.clear();

        final short PRIME = (short) 97;
        final short PRIME_SUM = sum0toN(PRIME);
        final Random rnd = new Random(0xDEADBEEF);

        // specific test for push or wrap around
        for (int step = 0; step < 100; step++) {
            for (short i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (short i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((short) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((short) 0, rb.evaluate());
        }

        // specific test for push & wrap around
        for (int step = 0; step < 100; step++) {
            for (short i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            for (short i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((short) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((short) 0, rb.evaluate());
        }

        // push random amounts and test
        for (int step = 0; step < 100; step++) {
            final short OFFSET = (short) rnd.nextInt((int) PRIME);

            for (short i = 0; i < OFFSET; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(sum0toN(OFFSET), rb.evaluate());

            for (short i = OFFSET; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (short i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((short) 0, rb.evaluate());
        }

        // pop random amounts and test
        for (int step = 0; step < 100; step++) {
            final short OFFSET = (short) rnd.nextInt((int) PRIME);
            for (short i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (short i = 0; i < OFFSET; i++) {
                rb.removeUnsafe();
            }
            assertEquals(PRIME_SUM - sum0toN(OFFSET), rb.evaluate());

            for (short i = 0; i < (PRIME - OFFSET); i++) {
                rb.removeUnsafe();
            }
            assertEquals((short) 0, rb.evaluate());
        }


        rb = new AggregatingShortRingBuffer(512, (short) 0, (a, b) -> (short) (a + b));
        // need to get the buffer to state where we have clean pushes and a wrapped pop

        // move the pointers close to the end
        for (short i = 0; i < 500; i++) {
            rb.addUnsafe(i);
            rb.removeUnsafe();
        }
        assertEquals((short) 0, rb.evaluate());
        assertEmpty(rb);

        // push past the end
        for (short i = 0; i < 200; i++) {
            rb.addUnsafe(i);
        }
        assertEquals(sum0toN((short) 200), rb.evaluate());

        // one more push to dirty the pushes
        rb.addUnsafe((short) 201);

        // pop past the end
        for (short i = 0; i < 200; i++) {
            rb.removeUnsafe();
        }

        // only thing in the buffer is the final push
        assertEquals((short) 201, rb.evaluate());
    }
    // endregion non-byte-tests

    public void testPushPopUnsafe() {
        final AggregatingShortRingBuffer rb =
                new AggregatingShortRingBuffer(3, (short) -Short.MAX_VALUE, (a, b) -> (short) Math.max(a, b));
        // move the head and tail off zero
        rb.ensureRemaining(100);
        for (short i = 0; i < 100; i++) {
            rb.addUnsafe(i);
        }
        for (short i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i);
        }

        // do it again with an offset
        rb.ensureRemaining(100);
        for (short i = 0; i < 100; i++) {
            rb.addUnsafe((short) (i + (short) 1000));
        }
        for (short i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i + (short) 1000);
        }

        for (short i = 0; i < 100; i++) {
            rb.addUnsafe((short) (i + (short) 1000));
        }
        rb.clear();

        for (short i = 0; i < 100; i++) {
            rb.add(i);
        }
        assertEquals((short) 99, rb.evaluate()); // last value added is max
    }

    public void testPopMultiple() {
        final AggregatingShortRingBuffer rb = new AggregatingShortRingBuffer(3, (short) 0, (a, b) -> (short) (a + b));

        for (int step = 0; step < 10; step++) {
            rb.ensureRemaining(100);
            for (short i = 0; i < 100; i++) {
                rb.addUnsafe(i);
            }

            if (step % 2 == 0) {
                rb.evaluate();
            }

            try {
                short[] values = rb.remove(101);
                fail("popping more values than size() should fail");
            } catch (NoSuchElementException x) {
                // expected
            }

            short[] values = rb.remove(100);
            for (short i = 0; i < 100; i++) {
                assertEquals(values[(int) i], i);
            }
            assertEmpty(rb);
        }
    }

    public void testSpecialCaseA() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingShortRingBuffer rb = new AggregatingShortRingBuffer(4, (short) 0, (a, b) -> (short) (a + b));
        rb.add((short) 1);
        assertEquals((short) 1, rb.remove());
        rb.add((short) 2);
        rb.add((short) 3);
        assertEquals((short) 5, rb.evaluate());
    }

    public void testSpecialCaseB() {
        // push the full capacity while wrapped
        final AggregatingShortRingBuffer rb = new AggregatingShortRingBuffer(64, (short) 0, (a, b) -> (short) (a + b));
        rb.add((short) 1);
        assertEquals((short) 1, rb.remove());

        for (int i = 0; i < 64; i++) {
            rb.add((short) 1);
        }
        assertEquals((short) 64, rb.evaluate());
    }

    public void testSpecialCaseC() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingShortRingBuffer rb = new AggregatingShortRingBuffer(16, (short) 0, (a, b) -> (short) (a + b));
        // move pointers to middle of storage
        for (int i = 0; i < 8; i++) {
            rb.add((short) 1);
            rb.remove();
        }
        assertEquals((short) 0, rb.evaluate());

        for (int i = 0; i < 11; i++) {
            rb.add((short) 1);
        }
        rb.remove();
        assertEquals((short) 10, rb.evaluate());
    }
}
