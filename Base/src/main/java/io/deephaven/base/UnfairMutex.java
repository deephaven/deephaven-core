/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class UnfairMutex {
    private static final Logger log = Logger.getLogger(UnfairMutex.class);
    private final int spinsUntilPark;
    private final String debugName;
    private final LockFreeArrayQueue<Thread> threads;
    private final AtomicReference<Thread> leader;
    private long lastLeadChange;

    public UnfairMutex(int spinsUntilPark) {
        this(null, spinsUntilPark);
    }

    public UnfairMutex(int spinsUntilPark, int threadCapacity) {
        this(null, spinsUntilPark, threadCapacity);
    }

    public UnfairMutex(String debugName, int spinsUntilPark) {
        this(debugName, spinsUntilPark, 1022); // Will convert to 10 in the primary constructor
    }

    public UnfairMutex(String debugName, int spinsUntilPark, int threadCapacity) {
        this.debugName = debugName;
        int capacityPower = Math.max(MathUtil.ceilLog2(threadCapacity + 2), 10); // miniumum of 1022
        this.threads = new LockFreeArrayQueue<Thread>(capacityPower);
        this.leader = new AtomicReference<Thread>(null);
        this.spinsUntilPark = spinsUntilPark;
    }

    public void lock() {
        boolean wasInterrupted = false;
        Thread t, me = Thread.currentThread();

        // long t0 = 0;
        // if ( debugName != null ) {
        // log.info("UnfairMutex: "+debugName+": thread "+me.getName()+" waiting");
        // t0 = System.nanoTime();
        // }

        int unfairSpins = 0;
        while (++unfairSpins < spinsUntilPark) {
            if (leader.compareAndSet(null, me)) {
                // if ( debugName != null ) {
                // lastLeadChange = System.nanoTime();
                // log.info("UnfairMutex: "+debugName+": UNFAIR thread "+me.getName()+" leading after "+((lastLeadChange
                // - t0 + 500) / 1000)+" micros");
                // }
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
        // if ( debugName != null ) {
        // lastLeadChange = System.nanoTime();
        // log.info("UnfairMutex: "+debugName+": thread "+me.getName()+" leading after "+((lastLeadChange - t0 + 500) /
        // 1000)+" micros");
        // }
    }

    public void unlock() {
        Thread me = Thread.currentThread();
        // if ( debugName != null ) {
        // log.info("UnfairMutex: "+debugName+": thread "+me.getName()+" handing off after "+((System.nanoTime() -
        // lastLeadChange + 500) / 1000)+" micros");
        // }
        if (!leader.compareAndSet(me, null)) {
            throw new IllegalStateException("wrong thread called handoff");
        }
        LockSupport.unpark(threads.peek());
    }

    public Thread getOwner() {
        return leader.get();
    }
}
