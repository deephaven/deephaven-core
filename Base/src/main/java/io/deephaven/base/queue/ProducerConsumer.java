/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.queue;

/**
 * Basis for LIFO, FIFO, Pool, and variants
 */
public interface ProducerConsumer<T> {
    /**
     * This method should never block (but it may spin for a finite amount of time) Returns true
     * when t was successfully produced, else false
     */
    boolean produce(T t);

    /**
     * This method should never block (but it may spin for a finite amount of time) Returns null
     * when there is nothing to consume [may create new objects on the fly if necessary]
     */
    T consume();

    /**
     * Must implement at least: One thread may call produce(), one thread may call consume()
     */
    public static interface SingleProducerConsumer<T> extends ProducerConsumer<T> {
    }

    /**
     * Must implement at least: Multiple threads may call produce(), one thread may call consume()
     */
    public static interface MultiProducer<T> extends SingleProducerConsumer<T> {
    }

    /**
     * Must implement at least: Multiple threads may call consume(), one thread may call produce()
     */
    public static interface MultiConsumer<T> extends SingleProducerConsumer<T> {
    }

    /**
     * Must implement at least: Multiple threads may call produce(), multiple threads may call
     * consume()
     */
    public static interface MultiProducerConsumer<T> extends MultiProducer<T>, MultiConsumer<T> {
    }
}
