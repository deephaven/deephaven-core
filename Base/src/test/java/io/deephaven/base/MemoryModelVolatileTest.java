/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import junit.framework.TestCase;

public class MemoryModelVolatileTest extends TestCase {
    private static class OddEven {
        private volatile boolean failed = false;

        public boolean failed() {
            return failed;
        }

        public void fail() {
            failed = true;
        }

        private volatile int wguard = 0;
        private int b = 0;
        private int c = 0;

        private final static long ratherFrequently = 1 << 10; // power of 2.
        private final static long rfmask = ratherFrequently - 1;
        private final static long ratherUnfrequently = 1 << 15; // power of 2, bigger than ratherFrequently.
        private final static long rumask = ratherUnfrequently - 1;

        void writeConsistently() {
            ++wguard;
            ++b;
            // hiccup every so often in the middle of a write.
            if ((b & rfmask) == 0) {
                if ((b & rumask) == 0) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                    }
                } else {
                    Thread.yield();
                }
            }
            ++c;
            ++wguard;
        }

        volatile int rguard = 0;

        boolean readConsistentlyAndCheck() {
            int localb, localc;
            do {
                do {
                    rguard = wguard;
                } while ((rguard & 1) == 1);
                localb = b;
                localc = c;
            } while (rguard != wguard);
            return localc == localb;
        }
    }

    public void testOddEven() {
        final OddEven oe = new OddEven();
        final long step = 1L << 20; // has to be power of 2.
        final long mask = step - 1;
        final long testLengthMillis = 10000L;

        final Thread writer = new Thread(new Runnable() {
            @Override
            public void run() {
                long pass = 0;
                final long start = System.currentTimeMillis();
                while (true) {
                    ++pass;
                    if ((pass & mask) == 0) {
                        if (System.currentTimeMillis() - start >= testLengthMillis || oe.failed()) {
                            return;
                        }
                    }
                    oe.writeConsistently();
                }
            }
        });

        final Thread reader = new Thread(new Runnable() {
            @Override
            public void run() {
                long pass = 0;
                final long start = System.currentTimeMillis();
                while (true) {
                    ++pass;
                    if ((pass & mask) == 0) {
                        if (System.currentTimeMillis() - start >= testLengthMillis) {
                            return;
                        }
                    }
                    if (!oe.readConsistentlyAndCheck()) {
                        oe.fail();
                        return;
                    }
                }
            }
        });

        writer.start();
        reader.start();
        while (reader.isAlive()) {
            try {
                reader.join();
            } catch (InterruptedException e) {
            }
        }
        while (writer.isAlive()) {
            try {
                writer.join();
            } catch (InterruptedException e) {
            }
        }
        assertFalse(oe.failed());
    }
}
