/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.queue;

/**
 * Common interface for LockFreeArrayQueue and variants.
 *
 * @param <T> the contained type
 */
public interface ConcurrentQueue<T> {
    /**
     * Returns false when the queue is full This method should never block (but it may spin for a finite amount of time)
     */
    boolean enqueue(T new_value);

    /**
     * Spins forever until the item can be enqueued. Calls yield() after the number of specified spins.
     */
    boolean enqueue(T new_value, long spins_between_yields);

    /**
     * Returns null when the queue is empty This method should never block (but it may spin for a finite amount of time)
     */
    T dequeue();

    /**
     * Only return when enqueued. (Might spin continuously)
     */
    void put(T new_value);

    /**
     * Only return w/ a dequeued value. (Might spin continuously)
     */
    T take();

    /**
     * Return the current next value, or null if the queue is empty.
     */
    T peek();
}
