/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm.util;

import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * A priority blocking queue that maintains one element per key. If a later request comes in of
 * higher priority, the lower priority item is replaced by the higher priority item.
 *
 * The element type must be usable in a hash map.
 */
public class KeyedPriorityBlockingQueue<E> {

    private static class Enqueued<E> implements Comparable<Enqueued> {
        private final long id;
        private final int priority;
        private final E element;
        private boolean isActive = true;

        private Enqueued(final long id, final int priority, final E element) {
            this.id = id;
            this.priority = priority;
            this.element = element;
        }


        @Override
        public int compareTo(@NotNull Enqueued o) {
            if (this == o) {
                return 0;
            }

            final int p = this.priority - o.priority;

            if (p != 0) {
                return -p;
            }

            return java.lang.Long.signum(this.id - o.id);
        }
    }

    private long nextId = 0;
    private final PriorityQueue<Enqueued<E>> fitterQueue = new PriorityQueue<>();
    private final Map<E, Enqueued> map = new HashMap<>();

    /**
     * Add an element to the queue. If the element is already in the queue, the element's priority
     * will be the higher of the existing element and the new element.
     *
     * @param element element to enqueue.
     * @param priority priority of the element.
     * @return {@code true} if the {@code element} was newly inserted to the queue or reinserted
     *         with a higher priority, otherwise {@code false}.
     */
    public synchronized boolean enqueue(final E element, final int priority) {
        final Enqueued e = map.get(element);

        if (e == null) {
            final Enqueued<E> ee = new Enqueued<>(nextId++, priority, element);
            fitterQueue.add(ee);
            map.put(element, ee);
            notify();
            return true;
        } else if (e.priority < priority) {
            final Enqueued<E> ee = new Enqueued<>(nextId++, priority, element);
            // marking as inactive and ignoring, rather than doing an O(N) remove
            e.isActive = false;
            fitterQueue.add(ee);
            map.put(element, ee);
            notify();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Retrieves and removes the head of this queue, waiting if necessary until an element becomes
     * available.
     *
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    public synchronized E take() throws InterruptedException {
        Enqueued<E> e = takeHead();

        // marking as inactive and ignoring, rather than doing an O(N) remove
        while (!e.isActive) {
            e = takeHead();
        }

        map.remove(e.element);
        return e.element;
    }

    private void checkInterrupt() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
    }

    private synchronized Enqueued<E> takeHead() throws InterruptedException {
        while (fitterQueue.isEmpty()) {
            wait();
            checkInterrupt();
        }

        checkInterrupt();
        return fitterQueue.poll();
    }

    /**
     * Returns true if the queue is empty; false otherwise.
     *
     * @return true if the queue is empty; false otherwise.
     */
    public synchronized boolean isEmpty() {
        return fitterQueue.isEmpty();
    }
}
