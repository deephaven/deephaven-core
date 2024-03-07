//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.testing;

import io.deephaven.base.*;
import junit.framework.Assert;

import java.util.function.Supplier;

public class MTTestSupport {

    public static <T> void assertBecomesEquals(int timeout, T expectedVal, Supplier<T> f) {
        long start = System.currentTimeMillis();
        long deadline = start + timeout;
        T testVal;
        for (;;) {
            if (expectedVal.equals(testVal = f.get())) {
                return;
            }
            long now = System.currentTimeMillis();
            if (now < deadline) {
                if (now - start >= 1000) {
                    start = now;
                    System.out.printf(
                            "assertBecomesEquals(%d millis, %s expected, %s actual) still waiting after %d millis%n",
                            timeout, expectedVal.toString(), testVal.toString(), now - (deadline - timeout));
                }
                Thread.yield();
                continue;
            }
            break;
        }
        Assert.fail("value did not become equal to " + expectedVal + " within " + timeout
                + " millis, most recent value was " + testVal);
    }

    public static <T> void assertRemainsEquals(int timeout, T val, Supplier<T> f) {
        long deadline = System.currentTimeMillis() + timeout;
        do {
            T sample = f.get();
            if (!val.equals(sample)) {
                Assert.fail("value did not remain equal to " + val + " for " + timeout
                        + " millis, most recent value was " + sample);
            }
            Thread.yield();
        } while (System.currentTimeMillis() < deadline);
    }

    public static void assertBecomesTrue(int timeout, Predicate.Nullary pred) {
        long deadline = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < deadline) {
            Thread.yield();
            if (pred.call()) {
                return;
            }
        }
        Assert.fail("predicate did not become true within " + timeout + " millis");
    }

    public static void assertRemainsTrue(int timeout, Predicate.Nullary pred) {
        long deadline = System.currentTimeMillis() + timeout;
        do {
            if (!pred.call()) {
                Assert.fail("predicate did not remain true for within " + timeout + " millis");
            }
            Thread.yield();
        } while (System.currentTimeMillis() < deadline);
    }

    public static void assertBecomesStable(int initialTimeout, int stableTimeout, Predicate.Nullary pred) {
        assertBecomesTrue(initialTimeout, pred);
        assertRemainsTrue(stableTimeout, pred);
    }

    public static void assertThreadTerminates(int timeout, Thread t) {
        assertThreadTerminates(timeout, t, () -> {
        });
    }

    public static void assertThreadTerminates(int timeout, Thread t, Runnable proc) {
        long deadline = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < deadline) {
            try {
                t.join(1);
                if (!t.isAlive()) {
                    return;
                }
            } catch (InterruptedException x) {
                // ignore
            }
            proc.run();
        }
        Assert.fail("Thread " + t + " did not terminate within " + timeout + " millis");
    }
}
