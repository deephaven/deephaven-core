/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import io.deephaven.base.queue.ConcurrentQueue;
import io.deephaven.base.queue.ProducerConsumer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A Java implementation of the algorithm described in:
 *
 * Philippas Tsigas, Yi Zhang, "A simple, fast and scalable non-blocking concurrent FIFO queue for
 * shared memory multiprocessor systems", Proceedings of the thirteenth annual ACM symposium on
 * Parallel algorithms and architectures, p.134-143, July 2001, Crete Island, Greece
 *
 * This version modifies the way we choose which NULL to use when dequeuing: 1) We let the head and
 * tail pointers range over the entire set of 32-bit unsigned values. We can convert a 32-bit
 * unsigned integer into a node index with the mod operator (or a bit mask, if we limit the queue
 * sizes to powers of two). 2) On each successive "pass" over the array, we want to alternate
 * between NULL(0) and NULL(1), that is, the first time the head pointer goes from zero to cap, we
 * replace dequeued values with NULL(0), then when head wraps back to zero we switch to using
 * NULL(1). Since we allow head to range over all 32-bit values, we can compute which null to use a
 * NULL((head / cap) % 2). If we are using powers of two, then the low-order bits [0,N] specify the
 * index into the nodes array, and bit N+1 specifies whether to use NULL(0) or NULL(1) when
 * dequeuing.
 */
public class LockFreeArrayQueue<T>
    implements ConcurrentQueue<T>, ProducerConsumer.MultiProducerConsumer<T> {
    /* private */ final int cap;
    // capacity of the queue - a power of two
    /* private */ final int mask;
    // mask to convert head/tail counters to node index
    /* private */ final int shift;
    // shift count to get null selection bit
    /* private */ final AtomicInteger head;
    /* private */ final AtomicInteger tail;
    /* private */ final AtomicReferenceArray<Object> nodes;

    private static final int LOG2CAP_MIN = 4;
    private static final int LOG2CAP_MAX = 28;

    /**
     * Get the minimum allowed queue capacity of this class.
     *
     * @return the minimum allowed capacity
     */
    public static int getMinAllowedCapacity() {
        return (1 << (LOG2CAP_MIN - 1)) + 1;
    }

    /**
     * Get the maximum allowed queue capacity of this class.
     *
     * @return the minimum allowed capacity
     */
    public static int getMaxAllowedCapacity() {
        return 1 << LOG2CAP_MAX;
    }

    // Basic characteristics:
    // a slot that contains NULL0 or NULL1 is empty, otherwise it is occupied
    // ((head + 1) % cap) is the next slot to be dequeued
    // (tail) is the next slot to be filled when enqueueing
    // if ((head + 1) % cap) == tail, then the queue is empty
    // if (tail == head), then the queue is full
    //
    // The algorithm allows the values in the head and tail fields to lag behind
    // their actual values in order to reduce the number of CASes that must be
    // performed. This means that enqueue and dequeue operations need to search
    // forwards (looking for null/non-null slots) to find the "real" head or tail.

    // We need two objects for nulls that will never be enqueued. We could just allocate
    // new objects, but we might as well just use head and tail AtomicInteger objects - since
    // they're private, you'd have to work pretty damn hard to attempt to enqueue them.
    private final Object[] NULLS = new Object[2];

    public LockFreeArrayQueue(int log2cap) {
        if (log2cap < LOG2CAP_MIN || log2cap > LOG2CAP_MAX) {
            throw new IllegalArgumentException("log2cap must be in [" + LOG2CAP_MIN + ","
                + LOG2CAP_MAX + "], got " + log2cap + ".");
        }
        this.cap = 1 << log2cap;
        this.mask = cap - 1;
        this.shift = log2cap;
        this.head = new AtomicInteger(0);
        this.tail = new AtomicInteger(1);

        this.NULLS[0] = new Object() {
            public String toString() {
                return "N0";
            }
        };
        this.NULLS[1] = new Object() {
            public String toString() {
                return "N1";
            }
        };

        this.nodes = new AtomicReferenceArray<Object>(cap);
        // on the first pass, we will use NULL(0), so init the array with NULL(1)
        for (int i = 0; i < cap; ++i) {
            this.nodes.set(i, NULLS[1]);
        }
        this.nodes.set(0, NULLS[0]);
    }

    public void init() {
        this.head.set(0);
        this.tail.set(1);
        for (int i = 0; i < cap; ++i) {
            this.nodes.set(i, NULLS[1]);
        }
        this.nodes.set(0, NULLS[0]);
    }

    public int capacity() {
        return cap;
    }

    private Object get_node(int n) {
        return nodes.get(n & mask);
    }

    private boolean cas_node(int n, Object old_val, Object new_val) {
        return nodes.compareAndSet(n & mask, old_val, new_val);
    }

    private Object get_null(int head) {
        return NULLS[(head >> shift) & 1];
    }

    private boolean same_slot(int a, int b) {
        return (a & mask) == (b & mask);
    }

    private boolean is_null(Object value) {
        return value == NULLS[0] || value == NULLS[1];
    }

    private int pass_number(int index) {
        return index >> shift;
    }

    @Override
    public boolean enqueue(T new_value) {
        if (new_value == null) {
            throw new IllegalArgumentException("TsigasZhangQueue cannot contain null elements");
        }
        while (true) {
            // initial value of the tail index - must not change while we are working
            final int tail0 = tail.get();
            // initial value of the head index
            final int head0 = head.get();
            // the actual tail of the queue - may be ahead of the tail index
            int actual_tail = tail0;
            // the old value in the slot where we enqueue the new value
            Object old_value = get_node(actual_tail);
            // the new tail (one past the slot we are going to enqueue)
            int new_tail = actual_tail + 1;

            // int debug_search_count = 0;

            // find the actual tail - the first slot containing a null
            while (!is_null(old_value)) {
                // check tail's consistency
                if (tail0 != tail.get()) {
                    break;
                }
                // if tail meets head, it's possible the queue is full
                if (same_slot(new_tail, head0)) {
                    break;
                }
                // now check the next cell
                actual_tail = new_tail;
                old_value = get_node(actual_tail);
                new_tail++;

                // debug_search_count++;
            }

            // if tail has changed, retry
            if (tail0 != tail.get()) {
                continue;
            }

            // if ( debug_search_count > cap/2 ) {
            // debug_stop();
            // synchronized ( this ) {
            // debug_dump(head.get(), tail.get(), head0, tail0, actual_tail, new_value,
            // debug_search_count);
            // }
            // debug_go();
            // }

            // debug_check("enqueue");

            // check whether queue is full
            if (same_slot(new_tail, head0)) {
                actual_tail = new_tail + 1;
                old_value = get_node(actual_tail);
                // the cell after head is occupied
                if (!is_null(old_value)) {
                    return false;
                }
                // help the dequeue to update head and retry
                head.compareAndSet(head0, head0 + 1);
                continue;
            }

            // check tail's consistency
            if (tail0 != tail.get()) {
                continue;
            }

            // now try to enqueue by CASing the new value into our slot
            if (cas_node(actual_tail, old_value, new_value)) {
                // enqueue has succeedded
                if (new_tail % 2 == 0) {
                    tail.compareAndSet(tail0, new_tail);
                }
                return true;
            }
        }
    }

    @Override
    public boolean enqueue(T new_value, long spins_between_yields) {
        // optimistic path
        if (enqueue(new_value)) {
            return true;
        }
        int spins = 0;
        while (!enqueue(new_value)) {
            if (++spins > spins_between_yields) {
                Thread.yield();
                spins = 0;
            }
        }
        return true;
    }

    public boolean enqueue(T new_value, long timeoutMicros, long maxSpins) {
        // optimistic path
        if (enqueue(new_value)) {
            return true;
        }
        int spins = 0;
        long t0 = System.nanoTime();
        long deadline = t0 + timeoutMicros * 1000;
        while (!enqueue(new_value)) {
            if (++spins > maxSpins) {
                if (System.nanoTime() > deadline) {
                    return false;
                }
                Thread.yield();
                spins = 0;
            }
        }
        return true;
    }

    @Override
    public T dequeue() {
        return do_dequeue(false, null, null);
    }

    @Override
    public T peek() {
        return do_dequeue(true, null, null);
    }

    public T dequeueThisObject(T expected) {
        return do_dequeue(false, expected, null);
    }

    public T dequeueIf(Predicate.Unary<T> predicate) {
        return do_dequeue(false, null, predicate);
    }

    private T do_dequeue(boolean peek, T expected, Predicate.Unary<T> predicate) {
        while (true) {
            // initial value of the head index - must not change while we are working
            final int head0 = head.get();
            // initial value of the tail index - most not change while we are searching for the
            // actual head
            final int tail0 = tail.get();
            // the slot from which we are going to dequeue
            int actual_head = head0 + 1;
            // the value we are dequeing
            Object val = get_node(actual_head);

            // find the actual head - the first slot containing a non-null
            while (is_null(val)) {
                // check head's consistency
                if (head0 != head.get()) {
                    break;
                }
                // if we find a null at the tail, the queue is empty
                if (same_slot(actual_head, tail0)) {
                    return null;
                }
                // look at the next slot
                actual_head++;
                val = get_node(actual_head);
            }
            // if head has changed, retry
            if (head0 != head.get()) {
                continue;
            }

            // debug_check("dequeue");

            // check whether queue is empty
            if (same_slot(actual_head, tail0)) {
                // help the enqueue update the tail
                tail.compareAndSet(tail0, tail0 + 1);
                continue;
            }

            // if dequeue rewinds to 0, switch null to avoid ABA
            Object tnull = get_null(actual_head);

            // if head has changed, retry
            if (head0 != head.get()) {
                continue;
            }

            // if we are peeking, we are done and can just return the value
            if (peek) {
                return (T) val;
            }
            if (expected != null && val != expected) {
                return null;
            }
            if (predicate != null && !predicate.call((T) val)) {
                return null;
            }

            // try to dequeue, by CASing the null into our slot
            if (cas_node(actual_head, val, tnull)) {
                // dequeue has succeeded, increment head (with lag)
                if (actual_head % 2 == 0) {
                    head.compareAndSet(head0, actual_head);
                }
                return (T) val;
            }
        }
    }

    @Override
    public void put(T new_value) {
        while (!enqueue(new_value)) {
        }
    }

    @Override
    public T take() {
        T deq;
        while ((deq = dequeue()) == null) {
        }
        return deq;
    }

    @Override
    public boolean produce(final T t) {
        return enqueue(t);
    }

    @Override
    public T consume() {
        return dequeue();
    }

    // ---------------------------------------------------------------------------------------------------
    // debugging code
    // ---------------------------------------------------------------------------------------------------

    private volatile boolean stopped = false;

    private void debug_stop() {
        // stopped = true;
    }

    private void debug_go() {
        // stopped = false;
    }

    private void debug_check(String what) {
        // int n = 0;
        // while ( stopped ) {
        // n++;
        // }
        // if ( n > 0 ) {
        // System.out.println(what+" held up for "+n+" spins in check()");
        // }
    }

    private void debug_dump(int h, int t, int h0, int t0, int at, Object new_value,
        int scan_count) {
        if (scan_count > 0) {
            System.out.println("LFAQ.enqueuing " + new_value + ": scanned " + scan_count
                + " slots looking for actual tail"
                + ", h0=" + h0 + "=" + (h0 % cap) + "/" + (h0 >> shift) + "/" + get_null(h0)
                + ", t0=" + t0 + "=" + (t0 % cap) + "/" + (t0 >> shift) + "/" + get_null(t0)
                + ", h=" + h + "=" + (h % cap) + "/" + (h >> shift) + "/" + get_null(h)
                + ", t=" + t + "=" + (t % cap) + "/" + (t >> shift) + "/" + get_null(t)
                + ", at=" + at + "=" + (at % cap) + "/" + (at >> shift) + "/" + get_null(at));
        }

        for (int i = 0; i < cap; ++i) {
            if (same_slot(i, t)) {
                System.out.print(" *T*");
            }
            if (same_slot(i, h)) {
                System.out.print(" *H*");
            }
            if (same_slot(i, t0)) {
                System.out.print(" *T0*");
            }
            if (same_slot(i, h0)) {
                System.out.print(" *H0*");
            }
            if (same_slot(i, at)) {
                System.out.print(" *AT*");
            }
            System.out.print(" " + get_node(i));
        }
        System.out.println();
    }
}
