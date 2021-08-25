/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import io.deephaven.base.verify.Assert;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class UnfairSemaphore {
    private final LockFreeArrayQueue<Thread> threads;
    private final AtomicInteger resource;
    private final int spinsUntilPark;

    public UnfairSemaphore(int resources, int spinsUntilPark) {
        this.spinsUntilPark = spinsUntilPark;
        threads = new LockFreeArrayQueue<Thread>(12); // 4094 threads max
        resource = new AtomicInteger(resources);
    }

    // careful w/ sizes > 1, might starve a guy waiting fora large number...
    // true means fast path, false means slow path
    public boolean acquire(int toAcquire) {
        return acquire(toAcquire, true);
    }

    public int acquireAll() {
        int acquired = tryAcquireAll();
        if (acquired <= 0) {
            acquire(1, false); // no need to unpark, we are taking it all right after this
            acquired = 1 + tryAcquireAll();
        }
        return acquired;
    }

    public int tryAcquireAll() {
        return AtomicUtil.getAndSetIfGreaterThan(resource, 0, 0);
    }

    public boolean tryAcquire(int toAcquire) {
        // we weren't unparked, so we don't need to unpark others
        return getAndDecreaseIfCan(toAcquire) >= toAcquire;
    }

    // returns the total amount of resources.
    // if return val >= toAcquire, it was decreased
    // else it wasn't
    private int getAndDecreaseIfCan(final int toAcquire) {
        return AtomicUtil.getAndDecreaseIfGreaterThan(resource, toAcquire, toAcquire - 1);
    }

    public int release(int toRelease) {
        final int r = resource.addAndGet(toRelease);
        LockSupport.unpark(threads.peek());
        return r;
    }

    public int releaseNoUnpark(int toRelease) {
        return resource.addAndGet(toRelease);
    }

    public int availablePermits() {
        return resource.get();
    }

    public int forceAcquire(int toAcquire) {
        return resource.addAndGet(-toAcquire);
    }

    // true means fast path, false means slow path
    private boolean acquire(int toAcquire, boolean doUnpark) {

        int unfairSpins = 0;
        while (unfairSpins++ < spinsUntilPark) {
            if (tryAcquire(toAcquire)) {
                // don't need to unpark others, we weren't unparked! (enqueued or unpark(peeked)!)
                return true;
            }
        }

        boolean wasInterrupted = false;

        final Thread t, me = Thread.currentThread();
        Assert.eqTrue(threads.enqueue(me), "threads.enqueue(me)");

        int resourcesAvailable;
        int spins = 0;
        boolean peekNotMe = true;
        while ((peekNotMe && (peekNotMe = (threads.peek() != me))) || // once we've peeked ourselves once, we don't need
                                                                      // to do it again!
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

        // we want to unpark, and there are resources left
        final int remainingResources = resourcesAvailable - toAcquire;
        if (doUnpark && remainingResources > 0) {
            LockSupport.unpark(threads.peek());
        }

        // reassert interrupt status on exit
        if (wasInterrupted) {
            me.interrupt();
        }

        return false;
    }
}
