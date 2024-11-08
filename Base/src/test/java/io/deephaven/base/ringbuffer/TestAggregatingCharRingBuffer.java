//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.ringbuffer;

import junit.framework.TestCase;

import java.util.NoSuchElementException;
import java.util.Random;

public class TestAggregatingCharRingBuffer extends TestCase {

    private void assertEmpty(AggregatingCharRingBuffer rb) {
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
        final AggregatingCharRingBuffer rb = new AggregatingCharRingBuffer(3, (char) 0, (a, b) -> (char) (a + b));

        // move the head and tail off zero
        for (int i = 0; i < 1000; i++) {
            rb.add((char) i);
        }
        for (int i = 0; i < 1000; i++) {
            rb.remove();
        }

        for (int i = 0; i < 10_000; i++)
            rb.add((char) i);

        for (int i = 10_000; i < 1_000_000; i++) {
            rb.add((char) i);
            assertEquals((char) (i - 10_000 + 1), rb.front(1));
            assertEquals((char) (i - 10_000), rb.remove());
            assertEquals(rb.remaining(), rb.capacity() - rb.size());
        }
    }

    public void testEvaluateMinLargeAmounts() {
        final AggregatingCharRingBuffer rb =
                new AggregatingCharRingBuffer(3, Character.MAX_VALUE, (a, b) -> (char) Math.min(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((char) i);

        final int maxVal = (int) Math.min(Character.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((char) i);
            assertEquals((char) (i - 10_000) + 1, rb.front(1));
            assertEquals((char) (i - 10_000), rb.evaluate()); // front of queue is min
            assertEquals((char) (i - 10_000), rb.remove());
        }
    }

    public void testEvaluateMaxLargeAmounts() {
        final AggregatingCharRingBuffer rb =
                new AggregatingCharRingBuffer(3, Character.MIN_VALUE, (a, b) -> (char) Math.max(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((char) i);

        final int maxVal = (int) Math.min(Character.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((char) i);
            assertEquals((char) (i - 10_000 + 1), rb.front(1));
            assertEquals((char) i, rb.evaluate()); // last value added is max
            assertEquals((char) (i - 10_000), rb.remove());
        }
    }

    // region non-byte-tests

    public void testEvaluateSumLargeAmounts() {
        final AggregatingCharRingBuffer rb = new AggregatingCharRingBuffer(3, (char) 0, (a, b) -> (char) (a + b));
        char runningSum = (char) 0;

        for (char i = 0; i < 1_000; i++) {
            rb.add(i);
            runningSum += i;
        }

        // stay small enough to avoid charing errors but prove the concept
        for (char i = 1_000; i < 10_000; i++) {
            rb.add(i);
            runningSum += i; // add the current value
            assertEquals((char) (i - 1_000 + 1), rb.front(1));

            assertEquals(runningSum, rb.evaluate());

            assertEquals((char) (i - 1_000), rb.remove());
            runningSum -= i - 1_000; // remove the value 1_0000 ago

            assertEquals(runningSum, rb.evaluate());
        }
    }

    /***
     * Return the sum of 0 to N-1
     */
    private char sum0toN(char n) {
        if (n == (char) 0) {
            return (char) 0; // not negative zero, sigh
        }
        return (char) (n * (n - 1) / 2);
    }

    public void testEvaluationEdgeCase() {
        AggregatingCharRingBuffer rb = new AggregatingCharRingBuffer(512, (char) 0, (a, b) -> (char) (a + b));

        rb.addIdentityValue();
        assertEquals((char) 0, rb.evaluate());
        rb.clear();

        final char PRIME = (char) 97;
        final char PRIME_SUM = sum0toN(PRIME);
        final Random rnd = new Random(0xDEADBEEF);

        // specific test for push or wrap around
        for (int step = 0; step < 100; step++) {
            for (char i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (char i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((char) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((char) 0, rb.evaluate());
        }

        // specific test for push & wrap around
        for (int step = 0; step < 100; step++) {
            for (char i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            for (char i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((char) 0, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals((char) 0, rb.evaluate());
        }

        // push random amounts and test
        for (int step = 0; step < 100; step++) {
            final char OFFSET = (char) rnd.nextInt((int) PRIME);

            for (char i = 0; i < OFFSET; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(sum0toN(OFFSET), rb.evaluate());

            for (char i = OFFSET; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (char i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals((char) 0, rb.evaluate());
        }

        // pop random amounts and test
        for (int step = 0; step < 100; step++) {
            final char OFFSET = (char) rnd.nextInt((int) PRIME);
            for (char i = 0; i < PRIME; i++) {
                rb.addUnsafe(i);
            }
            assertEquals(PRIME_SUM, rb.evaluate());

            for (char i = 0; i < OFFSET; i++) {
                rb.removeUnsafe();
            }
            assertEquals(PRIME_SUM - sum0toN(OFFSET), rb.evaluate());

            for (char i = 0; i < (PRIME - OFFSET); i++) {
                rb.removeUnsafe();
            }
            assertEquals((char) 0, rb.evaluate());
        }


        rb = new AggregatingCharRingBuffer(512, (char) 0, (a, b) -> (char) (a + b));
        // need to get the buffer to state where we have clean pushes and a wrapped pop

        // move the pointers close to the end
        for (char i = 0; i < 500; i++) {
            rb.addUnsafe(i);
            rb.removeUnsafe();
        }
        assertEquals((char) 0, rb.evaluate());
        assertEmpty(rb);

        // push past the end
        for (char i = 0; i < 200; i++) {
            rb.addUnsafe(i);
        }
        assertEquals(sum0toN((char) 200), rb.evaluate());

        // one more push to dirty the pushes
        rb.addUnsafe((char) 201);

        // pop past the end
        for (char i = 0; i < 200; i++) {
            rb.removeUnsafe();
        }

        // only thing in the buffer is the final push
        assertEquals((char) 201, rb.evaluate());
    }
    // endregion non-byte-tests

    public void testPushPopUnsafe() {
        final AggregatingCharRingBuffer rb =
                new AggregatingCharRingBuffer(3, (char) -Character.MAX_VALUE, (a, b) -> (char) Math.max(a, b));
        // move the head and tail off zero
        rb.ensureRemaining(100);
        for (char i = 0; i < 100; i++) {
            rb.addUnsafe(i);
        }
        for (char i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i);
        }

        // do it again with an offset
        rb.ensureRemaining(100);
        for (char i = 0; i < 100; i++) {
            rb.addUnsafe((char) (i + (char) 1000));
        }
        for (char i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i + (char) 1000);
        }

        for (char i = 0; i < 100; i++) {
            rb.addUnsafe((char) (i + (char) 1000));
        }
        rb.clear();

        for (char i = 0; i < 100; i++) {
            rb.add(i);
        }
        assertEquals((char) 99, rb.evaluate()); // last value added is max
    }

    public void testPopMultiple() {
        final AggregatingCharRingBuffer rb = new AggregatingCharRingBuffer(3, (char) 0, (a, b) -> (char) (a + b));

        for (int step = 0; step < 10; step++) {
            rb.ensureRemaining(100);
            for (char i = 0; i < 100; i++) {
                rb.addUnsafe(i);
            }

            if (step % 2 == 0) {
                rb.evaluate();
            }

            try {
                char[] values = rb.remove(101);
                fail("popping more values than size() should fail");
            } catch (NoSuchElementException x) {
                // expected
            }

            char[] values = rb.remove(100);
            for (char i = 0; i < 100; i++) {
                assertEquals(values[(int) i], i);
            }
            assertEmpty(rb);
        }
    }

    public void testSpecialCaseA() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingCharRingBuffer rb = new AggregatingCharRingBuffer(4, (char) 0, (a, b) -> (char) (a + b));
        rb.add((char) 1);
        assertEquals((char) 1, rb.remove());
        rb.add((char) 2);
        rb.add((char) 3);
        assertEquals((char) 5, rb.evaluate());
    }

    public void testSpecialCaseB() {
        // push the full capacity while wrapped
        final AggregatingCharRingBuffer rb = new AggregatingCharRingBuffer(64, (char) 0, (a, b) -> (char) (a + b));
        rb.add((char) 1);
        assertEquals((char) 1, rb.remove());

        for (int i = 0; i < 64; i++) {
            rb.add((char) 1);
        }
        assertEquals((char) 64, rb.evaluate());
    }

    public void testSpecialCaseC() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingCharRingBuffer rb = new AggregatingCharRingBuffer(16, (char) 0, (a, b) -> (char) (a + b));
        // move pointers to middle of storage
        for (int i = 0; i < 8; i++) {
            rb.add((char) 1);
            rb.remove();
        }
        assertEquals((char) 0, rb.evaluate());

        for (int i = 0; i < 11; i++) {
            rb.add((char) 1);
        }
        rb.remove();
        assertEquals((char) 10, rb.evaluate());
    }
}
