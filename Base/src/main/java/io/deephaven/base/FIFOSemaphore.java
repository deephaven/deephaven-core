/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import io.deephaven.base.verify.Assert;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class FIFOSemaphore {
    private final LockFreeArrayQueue<Thread> threads;
    private final AtomicInteger resource;
    private final int spinsUntilPark;

    public FIFOSemaphore(int spinsUntilPark, int resources) {
        this.spinsUntilPark = spinsUntilPark;
        threads = new LockFreeArrayQueue<Thread>(12); // 4094 threads max
        resource = new AtomicInteger(resources);
    }

    // may break fairness
    public int tryAcquireAll() {
        return AtomicUtil.getAndSetIfGreaterThan(resource, 0, 0);
    }

    // may break fairness
    public boolean tryAcquire(int toAcquire) {
        // we weren't unparked, so we don't need to unpark others
        return getAndDecreaseIfCan(toAcquire) >= toAcquire;
    }

    // careful w/ sizes > 1... the first guy in the queue waits until it can get them all
    public void acquire(int toAcquire) {
        acquire(toAcquire, true);
    }

    // returns the total amount of resources.
    // if return val >= toAcquire, it was decreased
    // else it wasn't
    private int getAndDecreaseIfCan(final int toAcquire) {
        return AtomicUtil.getAndDecreaseIfGreaterThan(resource, toAcquire, toAcquire - 1);
    }

    // may break fairness
    public int acquireAll() {
        int acquired = tryAcquireAll();
        if (acquired == 0) {
            acquire(1, false); // no need to unpark others, we are taking everything!
            acquired = 1 + tryAcquireAll();
        }
        return acquired;
    }

    public void release(int toRelease) {
        resource.getAndAdd(toRelease);
        LockSupport.unpark(threads.peek());
    }

    public int availablePermits() {
        return resource.get();
    }

    public void forceAcquire(int toAcquire) {
        resource.getAndAdd(-toAcquire);
    }

    private void acquire(int toAcquire, boolean doUnpark) {
        boolean wasInterrupted = false;
        Thread t, me = Thread.currentThread();
        Assert.eqTrue(threads.enqueue(me), "threads.enqueue(me)");

        int spins = 0;
        int resourcesAvailable;
        boolean peekNotMe = true;
        while ((peekNotMe && (peekNotMe = (threads.peek() != me))) || // once we've peeked ourselves
                                                                      // once, we don't need to do
                                                                      // it again!
            (resourcesAvailable = getAndDecreaseIfCan(toAcquire)) < toAcquire) {
            if ((++spins % spinsUntilPark) == 0) {
                LockSupport.park(this);

                // ignore interrupts while waiting
                if (Thread.interrupted()) {
                    wasInterrupted = true;
                }
            }
        }

        if ((t = threads.dequeue()) != me) {
            throw new IllegalStateException("Failed to dequeue myself, got " + t);
        }

        // notify the next guy if there are more resources available
        if (doUnpark && resourcesAvailable > toAcquire) {
            LockSupport.unpark(threads.peek());
        }

        // reassert interrupt status on exit
        if (wasInterrupted) {
            me.interrupt();
        }
    }
}
