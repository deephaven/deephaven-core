/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;


import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class UnfairMutex {
    private final int spinsUntilPark;
    private final LockFreeArrayQueue<Thread> threads;
    private final AtomicReference<Thread> leader;

    public UnfairMutex(int spinsUntilPark) {
        this(spinsUntilPark, 1022); // Will convert to 10 in the primary constructor
    }

    public UnfairMutex(int spinsUntilPark, int threadCapacity) {
        int capacityPower = Math.max(MathUtil.ceilLog2(threadCapacity + 2), 10); // miniumum of 1022
        this.threads = new LockFreeArrayQueue<Thread>(capacityPower);
        this.leader = new AtomicReference<Thread>(null);
        this.spinsUntilPark = spinsUntilPark;
    }

    public void lock() {
        boolean wasInterrupted = false;
        Thread t, me = Thread.currentThread();
        int unfairSpins = 0;
        while (++unfairSpins < spinsUntilPark) {
            if (leader.compareAndSet(null, me)) {
                return;
            }
        }

        while (!threads.enqueue(me)) {
            // wait
        }
        int spins = 0;
        boolean peekNotMe = true;
        while ((peekNotMe && (peekNotMe = (threads.peek() != me))) || // once we've peeked ourselves once, we don't need
                                                                      // to do it again!
                !leader.compareAndSet(null, me)) {
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
        // reassert interrupt status on exit
        if (wasInterrupted) {
            me.interrupt();
        }
    }

    public void unlock() {
        Thread me = Thread.currentThread();
        if (!leader.compareAndSet(me, null)) {
            throw new IllegalStateException("wrong thread called handoff");
        }
        LockSupport.unpark(threads.peek());
    }

    public Thread getOwner() {
        return leader.get();
    }
}
