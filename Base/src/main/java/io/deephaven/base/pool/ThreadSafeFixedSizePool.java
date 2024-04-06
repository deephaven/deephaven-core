//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.pool;

import io.deephaven.base.LockFreeArrayQueue;
import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Require;
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
public class ThreadSafeFixedSizePool<T> implements Pool<T> {

    public static Factory FACTORY = new Factory() {
        @Override
        public <T> Pool<T> create(int nSize, Supplier<T> itemFactory, Consumer<T> clearingProcedure) {
            return new ThreadSafeFixedSizePool<T>(Require.geq(nSize, "nSize", MIN_SIZE, "MIN_SIZE"),
                    Require.neqNull(itemFactory, "itemFactory"), clearingProcedure);
        }
    };

    public static final int MIN_SIZE = 7;

    private final static int SPIN_COUNT = 10000;

    protected final LockFreeArrayQueue<T> pool; // TODO: a stack would be nice here
    private final Consumer<T> clearingProcedure;

    public ThreadSafeFixedSizePool(int size, Supplier<T> factory,
            Consumer<T> clearingProcedure) {
        this(size, Require.neqNull(factory, "factory"), clearingProcedure, false);
    }

    protected ThreadSafeFixedSizePool(int size, Consumer<T> clearingProcedure) {
        this(size, null, clearingProcedure, false);
    }

    private ThreadSafeFixedSizePool(int size, @Nullable Supplier<T> factory,
            Consumer<T> clearingProcedure, boolean dummy) {
        Require.geq(size, "size", MIN_SIZE, "MIN_SIZE");
        this.clearingProcedure = clearingProcedure;
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
        int spins = 0;
        while (!pool.enqueue(item)) {
            if (++spins > SPIN_COUNT) {
                Thread.yield();
                spins = 0;
            }
        }
    }

    public T take() {
        T item = pool.dequeue();
        if (item != null) {
            // happy path
            return item;
        }
        int spins = 0;
        while ((item = pool.dequeue()) == null) {
            if (++spins > SPIN_COUNT) {
                Thread.yield();
                spins = 0;
            }
        }
        return item;
    }
}
