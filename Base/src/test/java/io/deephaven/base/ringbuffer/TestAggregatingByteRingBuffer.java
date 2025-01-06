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

public class TestAggregatingByteRingBuffer extends TestCase {

    private void assertEmpty(AggregatingByteRingBuffer rb) {
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
        final AggregatingByteRingBuffer rb = new AggregatingByteRingBuffer(3, (byte) 0, (a, b) -> (byte) (a + b));

        // move the head and tail off zero
        for (int i = 0; i < 1000; i++) {
            rb.add((byte) i);
        }
        for (int i = 0; i < 1000; i++) {
            rb.remove();
        }

        for (int i = 0; i < 10_000; i++)
            rb.add((byte) i);

        for (int i = 10_000; i < 1_000_000; i++) {
            rb.add((byte) i);
            assertEquals((byte) (i - 10_000 + 1), rb.front(1));
            assertEquals((byte) (i - 10_000), rb.remove());
            assertEquals(rb.remaining(), rb.capacity() - rb.size());
        }
    }

    public void testEvaluateMinLargeAmounts() {
        final AggregatingByteRingBuffer rb =
                new AggregatingByteRingBuffer(3, Byte.MAX_VALUE, (a, b) -> (byte) Math.min(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((byte) i);

        final int maxVal = (int) Math.min(Byte.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((byte) i);
            assertEquals((byte) (i - 10_000) + 1, rb.front(1));
            assertEquals((byte) (i - 10_000), rb.evaluate()); // front of queue is min
            assertEquals((byte) (i - 10_000), rb.remove());
        }
    }

    public void testEvaluateMaxLargeAmounts() {
        final AggregatingByteRingBuffer rb =
                new AggregatingByteRingBuffer(3, Byte.MIN_VALUE, (a, b) -> (byte) Math.max(a, b));
        for (int i = 0; i < 10_000; i++)
            rb.add((byte) i);

        final int maxVal = (int) Math.min(Byte.MAX_VALUE, 1_000_000);

        for (int i = 10_000; i < maxVal; i++) {
            rb.add((byte) i);
            assertEquals((byte) (i - 10_000 + 1), rb.front(1));
            assertEquals((byte) i, rb.evaluate()); // last value added is max
            assertEquals((byte) (i - 10_000), rb.remove());
        }
    }

    // region non-byte-tests
    // Tests removed due to limitations of byte storage
    // endregion non-byte-tests

    public void testPushPopUnsafe() {
        final AggregatingByteRingBuffer rb =
                new AggregatingByteRingBuffer(3, (byte) -Byte.MAX_VALUE, (a, b) -> (byte) Math.max(a, b));
        // move the head and tail off zero
        rb.ensureRemaining(100);
        for (byte i = 0; i < 100; i++) {
            rb.addUnsafe(i);
        }
        for (byte i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i);
        }

        // do it again with an offset
        rb.ensureRemaining(100);
        for (byte i = 0; i < 100; i++) {
            rb.addUnsafe((byte) (i + (byte) 1000));
        }
        for (byte i = 0; i < 100; i++) {
            assertEquals(rb.removeUnsafe(), i + (byte) 1000);
        }

        for (byte i = 0; i < 100; i++) {
            rb.addUnsafe((byte) (i + (byte) 1000));
        }
        rb.clear();

        for (byte i = 0; i < 100; i++) {
            rb.add(i);
        }
        assertEquals((byte) 99, rb.evaluate()); // last value added is max
    }

    public void testPopMultiple() {
        final AggregatingByteRingBuffer rb = new AggregatingByteRingBuffer(3, (byte) 0, (a, b) -> (byte) (a + b));

        for (int step = 0; step < 10; step++) {
            rb.ensureRemaining(100);
            for (byte i = 0; i < 100; i++) {
                rb.addUnsafe(i);
            }

            if (step % 2 == 0) {
                rb.evaluate();
            }

            try {
                byte[] values = rb.remove(101);
                fail("popping more values than size() should fail");
            } catch (NoSuchElementException x) {
                // expected
            }

            byte[] values = rb.remove(100);
            for (byte i = 0; i < 100; i++) {
                assertEquals(values[(int) i], i);
            }
            assertEmpty(rb);
        }
    }

    public void testSpecialCaseA() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingByteRingBuffer rb = new AggregatingByteRingBuffer(4, (byte) 0, (a, b) -> (byte) (a + b));
        rb.add((byte) 1);
        assertEquals((byte) 1, rb.remove());
        rb.add((byte) 2);
        rb.add((byte) 3);
        assertEquals((byte) 5, rb.evaluate());
    }

    public void testSpecialCaseB() {
        // push the full capacity while wrapped
        final AggregatingByteRingBuffer rb = new AggregatingByteRingBuffer(64, (byte) 0, (a, b) -> (byte) (a + b));
        rb.add((byte) 1);
        assertEquals((byte) 1, rb.remove());

        for (int i = 0; i < 64; i++) {
            rb.add((byte) 1);
        }
        assertEquals((byte) 64, rb.evaluate());
    }

    public void testSpecialCaseC() {
        // overlapping push and pop ranges with popTail < pushTail
        final AggregatingByteRingBuffer rb = new AggregatingByteRingBuffer(16, (byte) 0, (a, b) -> (byte) (a + b));
        // move pointers to middle of storage
        for (int i = 0; i < 8; i++) {
            rb.add((byte) 1);
            rb.remove();
        }
        assertEquals((byte) 0, rb.evaluate());

        for (int i = 0; i < 11; i++) {
            rb.add((byte) 1);
        }
        rb.remove();
        assertEquals((byte) 10, rb.evaluate());
    }
}
