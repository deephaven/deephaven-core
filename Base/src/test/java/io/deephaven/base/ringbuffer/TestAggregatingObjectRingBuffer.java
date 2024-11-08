//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.ringbuffer;

import junit.framework.TestCase;

import java.math.BigInteger;
import java.util.NoSuchElementException;
import java.util.Random;

public class TestAggregatingObjectRingBuffer extends TestCase {

    private void assertEmpty(AggregatingObjectRingBuffer rb) {
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
        final BigInteger id = BigInteger.valueOf(0);
        final AggregatingObjectRingBuffer<BigInteger> rb = new AggregatingObjectRingBuffer<>(3, id,
                BigInteger::add);

        // move the head and tail off zero
        for (int i = 0; i < 1000; i++) {
            rb.add(BigInteger.valueOf(i));
        }
        for (int i = 0; i < 1000; i++) {
            rb.remove();
        }

        for (int i = 0; i < 10_000; i++)
            rb.add(BigInteger.valueOf(i));

        for (int i = 10_000; i < 1_000_000; i++) {
            rb.add(BigInteger.valueOf(i));
            assertEquals(BigInteger.valueOf(i - 10_000 + 1), rb.front(1));
            assertEquals(BigInteger.valueOf(i - 10_000), rb.remove());
            assertEquals(rb.remaining(), rb.capacity() - rb.size());
        }
    }

    public void testEvaluateMinLargeAmounts() {
        final BigInteger id = BigInteger.valueOf(Long.MAX_VALUE);
        final AggregatingObjectRingBuffer<BigInteger> rb = new AggregatingObjectRingBuffer<>(3, id,
                BigInteger::min);
        for (int i = 0; i < 10_000; i++)
            rb.add(BigInteger.valueOf(i));

        for (int i = 10_000; i < 1_000_000; i++) {
            rb.add(BigInteger.valueOf(i));
            assertEquals(BigInteger.valueOf(i - 10_000 + 1), rb.front(1));
            assertEquals(BigInteger.valueOf(i - 10_000), rb.evaluate()); // front of queue is min
            assertEquals(BigInteger.valueOf(i - 10_000), rb.remove());
        }
    }

    public void testEvaluateMaxLargeAmounts() {
        final BigInteger id = BigInteger.valueOf(Long.MIN_VALUE);
        final AggregatingObjectRingBuffer<BigInteger> rb = new AggregatingObjectRingBuffer<>(3, id,
                BigInteger::max);
        for (int i = 0; i < 10_000; i++)
            rb.add(BigInteger.valueOf(i));

        for (int i = 10_000; i < 1_000_000; i++) {
            rb.add(BigInteger.valueOf(i));
            assertEquals(BigInteger.valueOf(i - 10_000 + 1), rb.front(1));
            assertEquals(BigInteger.valueOf(i), rb.evaluate()); // last value added is max
            assertEquals(BigInteger.valueOf(i - 10_000), rb.remove());
        }
    }

    // region non-byte-tests

    public void testEvaluateSumLargeAmounts() {
        final BigInteger id = BigInteger.valueOf(0);
        final AggregatingObjectRingBuffer<BigInteger> rb = new AggregatingObjectRingBuffer<>(3, id,
                BigInteger::add);
        BigInteger runningSum = BigInteger.valueOf(0);

        for (int i = 0; i < 1_000; i++) {
            BigInteger val = BigInteger.valueOf(i);
            rb.add(val);
            runningSum = runningSum.add(val);
        }

        // stay small enough to avoid Objecting errors but prove the concept
        for (int i = 1_000; i < 10_000; i++) {
            BigInteger val = BigInteger.valueOf(i);
            rb.add(val);
            runningSum = runningSum.add(val);

            assertEquals(BigInteger.valueOf(i - 1_000 + 1), rb.front(1));

            assertEquals(runningSum, rb.evaluate());

            assertEquals(BigInteger.valueOf(i - 1_000), rb.remove());
            runningSum = runningSum.subtract(BigInteger.valueOf(i - 1_000));

            assertEquals(runningSum, rb.evaluate());
        }
    }

    /***
     * Return the sum of 0 to N-1
     */
    private int sum0toN(int n) {
        if (n == 0) {
            return 0; // not negative zero, sigh
        }
        return (n * (n - 1) / 2);
    }

    public void testEvaluationEdgeCase() {
        final BigInteger id = BigInteger.valueOf(0);
        AggregatingObjectRingBuffer<BigInteger> rb = new AggregatingObjectRingBuffer<>(512, id,
                BigInteger::add);

        rb.addIdentityValue();
        assertEquals(id, rb.evaluate());
        rb.clear();

        final int PRIME = 97;
        final int PRIME_SUM = sum0toN(PRIME);
        final Random rnd = new Random(0xDEADBEEF);

        // specific test for push or wrap around
        for (int step = 0; step < 100; step++) {
            for (int i = 0; i < PRIME; i++) {
                rb.addUnsafe(BigInteger.valueOf(i));
            }
            assertEquals(BigInteger.valueOf(PRIME_SUM), rb.evaluate());

            for (int i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals(id, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals(id, rb.evaluate());
        }

        // specific test for push & wrap around
        for (int step = 0; step < 100; step++) {
            for (int i = 0; i < PRIME; i++) {
                rb.addUnsafe(BigInteger.valueOf(i));
            }
            for (int i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals(id, rb.evaluate());
            // not a copy/paste error, call this twice
            assertEquals(id, rb.evaluate());
        }

        // push random amounts and test
        for (int step = 0; step < 100; step++) {
            final int OFFSET = rnd.nextInt(PRIME);

            for (int i = 0; i < OFFSET; i++) {
                rb.addUnsafe(BigInteger.valueOf(i));
            }
            assertEquals(BigInteger.valueOf(sum0toN(OFFSET)), rb.evaluate());

            for (int i = OFFSET; i < PRIME; i++) {
                rb.addUnsafe(BigInteger.valueOf(i));
            }
            assertEquals(BigInteger.valueOf(PRIME_SUM), rb.evaluate());

            for (int i = 0; i < PRIME; i++) {
                rb.removeUnsafe();
            }
            assertEquals(id, rb.evaluate());
        }

        // pop random amounts and test
        for (int step = 0; step < 100; step++) {
            final int OFFSET = rnd.nextInt(PRIME);
            for (int i = 0; i < PRIME; i++) {
                rb.addUnsafe(BigInteger.valueOf(i));
            }
            assertEquals(BigInteger.valueOf(PRIME_SUM), rb.evaluate());

            for (int i = 0; i < OFFSET; i++) {
                rb.removeUnsafe();
            }
            assertEquals(BigInteger.valueOf(PRIME_SUM - sum0toN(OFFSET)), rb.evaluate());

            for (int i = 0; i < (PRIME - OFFSET); i++) {
                rb.removeUnsafe();
            }
            assertEquals(id, rb.evaluate());
        }


        rb.clear();
        // need to get the buffer to state where we have clean pushes and a wrapped pop

        // move the pointers close to the end
        for (int i = 0; i < 500; i++) {
            rb.addUnsafe(BigInteger.valueOf(i));
            rb.removeUnsafe();
        }
        assertEquals(id, rb.evaluate());
        assertEmpty(rb);

        // push past the end
        for (int i = 0; i < 200; i++) {
            rb.addUnsafe(BigInteger.valueOf(i));
        }
        assertEquals(BigInteger.valueOf(sum0toN(200)), rb.evaluate());

        // one more push to dirty the pushes
        rb.addUnsafe(BigInteger.valueOf(201));

        // pop past the end
        for (int i = 0; i < 200; i++) {
            rb.removeUnsafe();
        }

        // only thing in the buffer is the final push
        assertEquals(BigInteger.valueOf(201), rb.evaluate());
    }
    // endregion non-byte-tests

    public void testPushPopUnsafe() {
        final BigInteger id = BigInteger.valueOf(Long.MIN_VALUE);
        final AggregatingObjectRingBuffer<BigInteger> rb = new AggregatingObjectRingBuffer<>(512, id,
                BigInteger::max);

        // move the head and tail off zero
        rb.ensureRemaining(100);
        for (int i = 0; i < 100; i++) {
            rb.addUnsafe(BigInteger.valueOf(i));
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), BigInteger.valueOf(i));
        }

        // do it again with an offset
        rb.ensureRemaining(100);
        for (int i = 0; i < 100; i++) {
            rb.addUnsafe(BigInteger.valueOf(i + 1000));
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), BigInteger.valueOf(i + 1000));
        }

        for (int i = 0; i < 100; i++) {
            rb.addUnsafe(BigInteger.valueOf(i + 1000));
        }
        rb.clear();

        for (int i = 0; i < 100; i++) {
            rb.addUnsafe(BigInteger.valueOf(i));
        }
        assertEquals(BigInteger.valueOf(99), rb.evaluate()); // last value added is max
    }

    public void testPopMultiple() {
        final BigInteger id = BigInteger.valueOf(0);
        AggregatingObjectRingBuffer<BigInteger> rb = new AggregatingObjectRingBuffer<>(512, id,
                BigInteger::add);

        for (int step = 0; step < 10; step++) {
            rb.ensureRemaining(100);
            for (int i = 0; i < 100; i++) {
                rb.addUnsafe(BigInteger.valueOf(i));
            }

            if (step % 2 == 0) {
                rb.evaluate();
            }

            try {
                Object[] values = rb.remove(101);
                fail("popping more values than size() should fail");
            } catch (NoSuchElementException x) {
                // expected
            }

            Object[] values = rb.remove(100);
            for (int i = 0; i < 100; i++) {
                assertEquals(values[i], BigInteger.valueOf(i));
            }
            assertEmpty(rb);
        }
    }

    public void testSpecialCaseA() {
        // overlapping push and pop ranges with popTail < pushTail
        final BigInteger id = BigInteger.valueOf(0);
        AggregatingObjectRingBuffer<BigInteger> rb = new AggregatingObjectRingBuffer<>(4, id,
                BigInteger::add);
        rb.add(BigInteger.valueOf(1));
        assertEquals(BigInteger.valueOf(1), rb.remove());

        rb.add(BigInteger.valueOf(2));
        rb.add(BigInteger.valueOf(3));
        assertEquals(BigInteger.valueOf(5), rb.evaluate());
    }

    public void testSpecialCaseB() {
        // push the full capacity while wrapped
        final BigInteger id = BigInteger.valueOf(0);
        AggregatingObjectRingBuffer<BigInteger> rb = new AggregatingObjectRingBuffer<>(64, id,
                BigInteger::add);
        rb.add(BigInteger.valueOf(1));
        assertEquals(BigInteger.valueOf(1), rb.remove());

        for (int i = 0; i < 64; i++) {
            rb.add(BigInteger.valueOf(1));
        }
        assertEquals(BigInteger.valueOf(64), rb.evaluate());
    }

    public void testSpecialCaseC() {
        // overlapping push and pop ranges with popTail < pushTail
        final BigInteger id = BigInteger.valueOf(0);
        AggregatingObjectRingBuffer<BigInteger> rb = new AggregatingObjectRingBuffer<>(16, id,
                BigInteger::add);
        // move pointers to middle of storage
        for (int i = 0; i < 8; i++) {
            rb.add(BigInteger.valueOf(1));
            rb.remove();
        }
        assertEquals(BigInteger.valueOf(0), rb.evaluate());

        for (int i = 0; i < 11; i++) {
            rb.add(BigInteger.valueOf(1));
        }
        rb.remove();
        assertEquals(BigInteger.valueOf(10), rb.evaluate());
    }
}
