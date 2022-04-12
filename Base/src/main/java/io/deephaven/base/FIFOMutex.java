/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class FIFOMutex {
    private final LockFreeArrayQueue<Thread> threads;
    private final AtomicReference<Thread> leader;

    public FIFOMutex() {
        this.threads = new LockFreeArrayQueue<Thread>(10); // 1022 threads max
        this.leader = new AtomicReference<Thread>(null);
    }

    public void lock() {
        boolean wasInterrupted = false;
        Thread t, me = Thread.currentThread();
        while (!threads.enqueue(me)) {
            // wait
        }
        int spins = 0;
        boolean peekNotMe = true;
        while ((peekNotMe && (peekNotMe = (threads.peek() != me))) || // once we've peeked ourselves once, we don't need
                                                                      // to do it again!
                !leader.compareAndSet(null, me)) {
            if ((++spins % 1000) == 0) {
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
