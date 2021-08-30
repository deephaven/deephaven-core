/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.queue;

import io.deephaven.base.UnfairSemaphore;
import io.deephaven.base.verify.Assert;

public class ProducerConsumerWithTake<T> implements ProducerConsumer<T> {
    private final ProducerConsumer<T> producerConsumer;
    private final UnfairSemaphore semaphore;

    public ProducerConsumerWithTake(final ProducerConsumer<T> producerConsumer) {
        this.producerConsumer = producerConsumer;
        semaphore = new UnfairSemaphore(0, 1000);
    }

    public boolean produce(final T t) {
        if (producerConsumer.produce(t)) {
            semaphore.release(1);
            return true;
        }
        return false;
    }

    @Override
    public T consume() {
        if (semaphore.tryAcquire(1)) {
            final T t = producerConsumer.consume();
            Assert.neqNull(t, "t");
            return t;
        }
        return null;
    }

    public T take() {
        semaphore.acquire(1);
        final T t = producerConsumer.consume();
        Assert.neqNull(t, "t");
        return t;
    }

    public int size() {
        return semaphore.availablePermits();
    }
}
