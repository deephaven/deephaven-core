/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.queue;

public interface ConcurrentPriorityQueue<T> extends ConcurrentQueue<T> {
    /**
     * 0 is normal priority negative is lower priority positive is more priority
     */

    boolean enqueue(T new_value, int priority);

    void put(T new_value, int priority);
}
