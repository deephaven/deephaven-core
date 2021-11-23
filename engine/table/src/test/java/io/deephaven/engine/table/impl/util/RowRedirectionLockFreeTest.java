/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.impl.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.LogicalClock;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Arrays;
import java.util.Random;
import org.junit.experimental.categories.Category;

import static io.deephaven.base.ArrayUtil.swap;

@Category(OutOfBandTest.class)
public class RowRedirectionLockFreeTest extends RefreshingTableTestCase {
    private static final long oneBillion = 1000000000L;
    private static final int testDurationInSeconds = 15;

    public void testRowRedirection() throws InterruptedException {
        final WritableRowRedirectionLockFree index = new RowRedirectionLockFreeFactory().createRowRedirection(10);
        index.startTrackingPrevValues();
        final long initialStep = LogicalClock.DEFAULT.currentStep();
        Writer writer = new Writer("writer", initialStep, index);
        Reader r0 = new Reader("reader0", initialStep, index);
        Reader r1 = new Reader("reader1", initialStep, index);

        // Run one iteration of the writer so the prev values exist.
        writer.doOneIteration();

        RWBase[] participants = {writer, r0, r1};
        Thread[] threads = Arrays.stream(participants).map(Thread::new).toArray(Thread[]::new);
        for (Thread thread : threads) {
            thread.start();
        }
        System.out.printf("Test will run for %d seconds%n", testDurationInSeconds);
        Thread.sleep(testDurationInSeconds * 1000);

        for (RWBase rwb : participants) {
            rwb.cancel();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        boolean failed = false;
        for (RWBase rwb : participants) {
            System.out.println(rwb);
            failed |= rwb.hasFailed();
        }
        if (failed) {
            fail("WritableRowRedirection had some corrupt values");
        }
    }

    private interface Cancellable {
        void cancel();
    }

    private static abstract class RWBase implements Runnable, Cancellable {
        protected final String name;
        protected final long initialStep;
        protected final WritableRowRedirectionLockFree index;
        protected int numIterations;
        protected volatile boolean cancelled;

        protected RWBase(String name, long initialStep, WritableRowRedirectionLockFree index) {
            this.name = name;
            this.initialStep = initialStep;
            this.index = index;
            this.numIterations = 0;
            this.cancelled = false;
        }

        public final void run() {
            while (!cancelled) {
                doOneIteration();
                ++numIterations;
            }
        }

        public final void cancel() {
            this.cancelled = true;
        }

        protected abstract void doOneIteration();

        public abstract boolean hasFailed();

    }

    private static class Reader extends RWBase {
        private int goodIdleCycles;
        private int goodUpdateCycles;
        private int badIdleCycles;
        private int badUpdateCycles;
        private int incoherentCycles;

        Reader(String name, long initialStep, WritableRowRedirectionLockFree index) {
            super(name, initialStep, index);
            goodIdleCycles = 0;
            goodUpdateCycles = 0;
            badIdleCycles = 0;
            badUpdateCycles = 0;
            incoherentCycles = 0;
        }

        @Override
        protected final void doOneIteration() {
            // Figure out what step we're in and what step to read from (current or prev).
            final long logicalClockStartValue = LogicalClock.DEFAULT.currentValue();
            final long stepFromCycle = LogicalClock.getStep(logicalClockStartValue);
            final LogicalClock.State state = LogicalClock.getState(logicalClockStartValue);
            final long step = state == LogicalClock.State.Updating ? stepFromCycle - 1 : stepFromCycle;

            final int keysInThisGeneration = (int) ((step - initialStep) * 1000 + 1000);
            final Random rng = new Random(step);
            final int numKeysToInsert = rng.nextInt(keysInThisGeneration);
            long[] keys = fillAndShuffle(rng, keysInThisGeneration);
            final WritableRowRedirectionLockFree ix = index;

            // Record the mismatches
            final TLongArrayList mmKeys = new TLongArrayList();
            final TLongArrayList mmExpect = new TLongArrayList();
            final TLongArrayList mmActual = new TLongArrayList();

            // Look at the map in the reverse order of the writer, just to avoid any unintended synchronization.
            // These keys are expected to not exist.
            for (int ii = keys.length - 1; ii >= numKeysToInsert; --ii) {
                final long key = keys[ii];
                final long actualValue = state == LogicalClock.State.Updating ? ix.getPrev(key) : ix.get(key);
                if (actualValue != -1) {
                    mmKeys.add(key);
                    mmExpect.add(-1);
                    mmActual.add(actualValue);
                }
            }

            // These keys are expected to exist
            for (int ii = numKeysToInsert - 1; ii >= 0; --ii) {
                final long key = keys[ii];
                final long expectedValue = step * oneBillion + ii;
                final long actualValue = state == LogicalClock.State.Updating ? ix.getPrev(key) : ix.get(key);
                if (expectedValue != actualValue) {
                    mmKeys.add(key);
                    mmExpect.add(expectedValue);
                    mmActual.add(actualValue);
                }
            }


            final long logicalClockEndValue = LogicalClock.DEFAULT.currentValue();
            if (logicalClockStartValue != logicalClockEndValue) {
                ++incoherentCycles;
                return;
            }
            if (mmKeys.isEmpty()) {
                if (state == LogicalClock.State.Updating) {
                    ++goodUpdateCycles;
                } else {
                    ++goodIdleCycles;
                }
                return;
            }
            if (state == LogicalClock.State.Updating) {
                ++badUpdateCycles;
            } else {
                ++badIdleCycles;
            }
        }

        @Override
        public boolean hasFailed() {
            return badIdleCycles != 0 || badUpdateCycles != 0;
        }

        @Override
        public String toString() {
            return String.format(
                    "--- %s: iterations: %d, good update: %d, good idle: %d, bad update: %d, bad idle: %d, incoherent (no judgment): %d ---",
                    name, numIterations, goodUpdateCycles, goodIdleCycles, badUpdateCycles, badIdleCycles,
                    incoherentCycles);
        }
    }

    private static class Writer extends RWBase {
        Writer(String name, long initialStep, WritableRowRedirectionLockFree index) {
            super(name, initialStep, index);
        }

        @Override
        protected final void doOneIteration() {
            final MutableInt keysInThisGeneration = new MutableInt();
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                final long step = LogicalClock.DEFAULT.currentStep();
                keysInThisGeneration.setValue((int) ((step - initialStep) * 1000 + 1000));
                final Random rng = new Random(step);
                final int numKeysToInsert = rng.nextInt(keysInThisGeneration.getValue());
                // A bit of a waste because we only look at the first 'numKeysToInsert' keys, but that's ok.
                long[] keys = fillAndShuffle(rng, keysInThisGeneration.getValue());
                final WritableRowRedirectionLockFree ix = index;
                for (int ii = 0; ii < numKeysToInsert; ++ii) {
                    final long key = keys[ii];
                    final long value = step * oneBillion + ii;
                    ix.put(key, value);
                }
                for (int ii = numKeysToInsert; ii < keys.length; ++ii) {
                    final long key = keys[ii];
                    ix.remove(key);
                }
            });

            // waste some time doing something else
            final WritableRowRedirectionLockFree privateIndex =
                    new RowRedirectionLockFreeFactory().createRowRedirection(10);
            for (long ii = 0; ii < keysInThisGeneration.getValue() * 4; ++ii) {
                privateIndex.put(ii, ii);
            }
        }

        @Override
        public String toString() {
            return String.format("+++ %s: iterations: %d +++", name, numIterations);
        }

        public boolean hasFailed() {
            return false;
        }
    }

    private static long[] fillAndShuffle(Random rng, int length) {
        long[] result = new long[length];
        for (int ii = 0; ii < result.length; ++ii) {
            result[ii] = ii;
        }
        for (int size = length; size > 1; --size) {
            int target = rng.nextInt(size);
            swap(result, target, size - 1);
        }
        return result;
    }
}
