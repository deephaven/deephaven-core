//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.stats;

import io.deephaven.base.AtomicUtil;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A thread-safe extension of the {@link Value} class.
 *
 * <p>
 * The {@link #sample(long)} method is synchronized, so may introduce contention compared to the unsafe Value version of
 * sample.
 * </p>
 */
public abstract class ThreadSafeValue extends Value {

    public ThreadSafeValue(long now) {
        super(now);
    }

    protected ThreadSafeValue(History history) {
        super(history);
    }

    @Override
    public synchronized void sample(final long x) {
        super.sample(x);
    }

    @Override
    public synchronized String toString() {
        return super.toString();
    }
}
