//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.stats;

import junit.framework.TestCase;

import java.util.concurrent.Semaphore;
import java.util.function.LongFunction;

// --------------------------------------------------------------------
/**
 * Tests for {@link Value}, {@link State}, {@link Counter}, and {@link History}
 */
public class TestValue extends TestCase {

    private static final int[] SAMPLES = new int[] {3, 1, 5, 2, 4};

    // ----------------------------------------------------------------
    public void testState() {
        State state = State.FACTORY.apply(0L);
        assertEquals('S', state.getTypeTag());
        for (int nSample : SAMPLES) {
            state.sampleFromIncrement(nSample); // 3, 4, 9, 11, 15
        }
        assertEquals(5, state.getN());
        assertEquals(15, state.getLast());
        assertEquals(42, state.getSum());
        assertEquals(452, state.getSum2());
        assertEquals(3, state.getMin());
        assertEquals(15, state.getMax());

        checkValue(State.FACTORY);
    }

    // ----------------------------------------------------------------
    public void testCounter() {
        Counter counter = Counter.FACTORY.apply(0L);
        assertEquals('C', counter.getTypeTag());
        for (int nSample : SAMPLES) {
            counter.incrementFromSample(nSample); // -2, 4, -3, 2
        }
        assertEquals(4, counter.getN());
        assertEquals(2, counter.getLast());
        assertEquals(1, counter.getSum());
        assertEquals(33, counter.getSum2());
        assertEquals(-3, counter.getMin());
        assertEquals(4, counter.getMax());

        checkValue(Counter.FACTORY);
    }

    public void testToString() {
        // this is purposefully a heisentest, this should make it fail if it is really broken
        for (int ii = 0; ii < 10; ++ii) {
            doTestToString();
        }
    }

    public void doTestToString() {
        // we are testing toString, but also creating a pile of threads to exercise some of the AtomicFieldUpdater
        // behavior of the value
        final Value counter = ThreadSafeCounter.FACTORY.apply(0L);

        final String emptyString = counter.toString();
        assertEquals("Value{n=0}", emptyString);

        final Semaphore semaphore = new Semaphore(0);
        final Semaphore completion = new Semaphore(0);
        final int n = 100;
        for (int ii = 0; ii < n; ++ii) {
            final int fii = ii;
            new Thread(() -> {
                semaphore.acquireUninterruptibly();
                counter.sample(fii);
                completion.release();
            }).start();
        }
        semaphore.release(100);
        completion.acquireUninterruptibly(100);

        assertEquals(n, counter.getN());
        assertEquals(((n - 1) * n) / 2, counter.getSum());
        assertEquals(0, counter.getMin());
        assertEquals(99, counter.getMax());

        final String asString = counter.toString();
        assertEquals("Value{n=100, sum=4,950, max=99, min=0, avg=49.500, std=29.011}", asString);
    }

    // ----------------------------------------------------------------
    private void checkValue(LongFunction<? extends Value> factory) {
        Value value = factory.apply(1000L);

        for (int nSample : SAMPLES) {
            value.sample(nSample);
        }
        assertEquals(5, value.getN());
        assertEquals(4, value.getLast());
        assertEquals(15, value.getSum());
        assertEquals(55, value.getSum2());
        assertEquals(1, value.getMin());
        assertEquals(5, value.getMax());

        History history = value.getHistory();
        // issue: actually, interval 0 did not turn over, so this should probably return -1 (and fix Value.update too)
        assertEquals(0, history.update(value, 1000L));

        assertEquals(5, history.getN(History.INTERVAL_1S_INDEX, 0));
        assertEquals(4, history.getLast(History.INTERVAL_1S_INDEX, 0));
        assertEquals(15, history.getSum(History.INTERVAL_1S_INDEX, 0));
        assertEquals(55, history.getSum2(History.INTERVAL_1S_INDEX, 0));
        assertEquals(1, history.getMin(History.INTERVAL_1S_INDEX, 0));
        assertEquals(5, history.getMax(History.INTERVAL_1S_INDEX, 0));
        assertEquals(3, history.getAvg(History.INTERVAL_1S_INDEX, 0));
        assertEquals(2, history.getStdev(History.INTERVAL_1S_INDEX, 0));

        // test history turn over
        // test safety on avg and stdev
        // test value.update()
    }
}
