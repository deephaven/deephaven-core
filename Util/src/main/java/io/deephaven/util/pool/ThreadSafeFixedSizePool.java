/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.pool;

import io.deephaven.base.LockFreeArrayQueue;
import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Require;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A pool that
 * <UL>
 * <LI>holds at least <code>size</code> items,
 * <LI>creates <code>size</code> items in the pool immediately,
 * <LI>blocks (busily) whenever the pool overflows or underflows,
 * <LI>optionally clears the items given to it, and
 * <LI>IS thread-safe
 * </UL>
 */
public class ThreadSafeFixedSizePool<T> implements PoolEx<T> {

    public static final int MIN_SIZE = 7;

    private final static int SPIN_COUNT = 10000;

    protected final LockFreeArrayQueue<T> pool; // TODO: a stack would be nice here
    private final Consumer<T> clearingProcedure;
    private final String logPfx;
    private final Logger log;

    private ThreadSafeFixedSizePool(int size, @Nullable Supplier<T> factory,
            Consumer<T> clearingProcedure, Logger log, String logPfx) {
        Require.geq(size, "size", MIN_SIZE, "MIN_SIZE");
        Require.requirement((log == null) == (logPfx == null),
                "log and logPfx must either both be null, or both non-null");
        this.clearingProcedure = clearingProcedure;
        this.log = log;
        this.logPfx = logPfx;
        this.pool = new LockFreeArrayQueue<T>(MathUtil.ceilLog2(size + 2));
        if (factory == null) {
            // If factory is null, we expect to have all items supplied via give().
            return;
        }
        for (int i = 0; i < size; ++i) {
            T element = factory.get();
            while (!pool.enqueue(element)) {
                // spin
            }
        }
    }

    public ThreadSafeFixedSizePool(int size, Supplier<T> factory, Consumer<T> clearingProcedure) {
        this(size, factory, clearingProcedure, null, null);
    }

    volatile long nextGiveLog = 0;

    public void give(T item) {
        if (null == item) {
            return;
        }
        if (null != clearingProcedure) {
            clearingProcedure.accept(item);
        }
        if (pool.enqueue(item)) {
            // happy path
            return;
        }
        int spins = 0, yields = 0;
        long t0 = log != null ? System.nanoTime() / 1000 : 0;
        try {
            while (!pool.enqueue(item)) {
                if (++spins > SPIN_COUNT) {
                    yields++;
                    if (log != null) {
                        long now = System.nanoTime() / 1000;
                        if (now > nextGiveLog) {
                            nextGiveLog = (now + 100000) - (now % 100000);
                            long dt = (now - t0);
                            log.warn().append(logPfx).append(": give() can't enqueue returned item, yield count = ")
                                    .append(yields).endl();
                        }
                    }
                    Thread.yield();
                    spins = 0;
                }
            }
        } finally {
            if (log != null) {
                long now = System.nanoTime() / 1000;
                if (now > nextGiveLog) {
                    nextGiveLog = (now + 100000) - (now % 100000);
                    long dt = (now - t0);
                    log.warn().append(logPfx).append(": give() took ").append(dt).append(" micros, with ")
                            .append(yields).append(" yields and ").append(spins).append(" additional spins").endl();
                }
            }
        }
    }

    volatile long nextTakeLog = 0;

    public T take() {
        T item = pool.dequeue();
        if (item != null) {
            // happy path
            return item;
        }
        int spins = 0, yields = 0;
        long t0 = log != null ? System.nanoTime() / 1000 : 0;
        try {
            while ((item = pool.dequeue()) == null) {
                if (++spins > SPIN_COUNT) {
                    yields++;
                    if (log != null) {
                        long now = System.nanoTime() / 1000;
                        if (now > nextTakeLog) {
                            nextTakeLog = (now + 100000) - (now % 100000);
                            long dt = (now - t0);
                            log.warn().append(logPfx).append(": take() can't dequeue from pool, waiting for ")
                                    .append(dt).append(" micros, yield count = ").append(yields).endl();
                        }
                    }
                    Thread.yield();
                    spins = 0;
                }
            }
            return item;
        } finally {
            if (log != null) {
                long now = System.nanoTime() / 1000;
                if (now > nextTakeLog) {
                    nextTakeLog = (now + 100000) - (now % 100000);
                    long dt = (now - t0);
                    log.warn().append(logPfx).append(": take() took ").append(dt).append(" micros, with ")
                            .append(yields).append(" yields and ").append(spins).append(" additional spins").endl();
                }
            }
        }
    }

    @Override
    public T tryTake() {
        return pool.dequeue();
    }
}
