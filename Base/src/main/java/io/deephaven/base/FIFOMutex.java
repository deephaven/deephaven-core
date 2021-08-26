/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class FIFOMutex {
    private static final Logger log = Logger.getLogger(FIFOMutex.class);
    private final String debugName;
    private final LockFreeArrayQueue<Thread> threads;
    private final AtomicReference<Thread> leader;
    private long lastLeadChange;

    public FIFOMutex() {
        this(null);
    }

    public FIFOMutex(String debugName) {
        this.debugName = debugName;
        this.threads = new LockFreeArrayQueue<Thread>(10); // 1022 threads max
        this.leader = new AtomicReference<Thread>(null);
    }

    public void lock() {
        boolean wasInterrupted = false;
        Thread t, me = Thread.currentThread();
        while (!threads.enqueue(me)) {
            // wait
        }
        long t0 = 0;
        if (debugName != null) {
            log.info("FIFOMutex: " + debugName + ": thread " + me.getName() + " waiting");
            t0 = System.nanoTime();
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
        if (debugName != null) {
            lastLeadChange = System.nanoTime();
            log.info("FIFOMutex: " + debugName + ": thread " + me.getName() + " leading after "
                    + ((lastLeadChange - t0 + 500) / 1000) + " micros");
        }
    }

    public void unlock() {
        Thread me = Thread.currentThread();
        if (debugName != null) {
            log.info("FIFOMutex: " + debugName + ": thread " + me.getName() + " handing off after "
                    + ((System.nanoTime() - lastLeadChange + 500) / 1000) + " micros");
        }
        if (!leader.compareAndSet(me, null)) {
            throw new IllegalStateException("wrong thread called handoff");
        }
        LockSupport.unpark(threads.peek());
    }

    public Thread getOwner() {
        return leader.get();
    }
}
