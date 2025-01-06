//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.pool;

import io.deephaven.base.LockFreeArrayQueue;
import io.deephaven.base.MathUtil;
import io.deephaven.base.pool.Pool;
import io.deephaven.base.stats.Counter;
import io.deephaven.base.stats.Stats;
import io.deephaven.base.verify.Require;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A pool that
 * <UL>
 * <LI>holds at least <code>size</code> items,
 * <LI>creates <code>size</code> items in the pool immediately,
 * <LI>creates a new item when the pool underflows,
 * <LI>discards the item when the pool overflows,
 * <LI>optionally clears the items given to it, and
 * <LI>IS thread-safe
 * </UL>
 */
public class ThreadSafeLenientFixedSizePool<T> implements Pool.MultiPool<T>, PoolEx<T> {
    /**
     * Convert a {@link Supplier} into a {@link Function} that ignores the argument.
     *
     * @param supplier The supplier
     * @param <T> the type to be returned
     * @return a Function taking a ThreadSafeLenientFixedSizePool(ignored) and returning T
     */
    private static <T> Function<ThreadSafeLenientFixedSizePool<T>, T> makeFactoryAdapter(
            final Supplier<T> supplier) {
        return arg -> supplier.get();
    }

    public static final int MIN_SIZE = 7;

    private final LockFreeArrayQueue<T> pool; // TODO: should be a stack
    private final Function<ThreadSafeLenientFixedSizePool<T>, T> factory;
    private final Consumer<? super T> clearingProcedure;
    private final Counter extraFactoryCalls;

    public ThreadSafeLenientFixedSizePool(String name,
            int size,
            Supplier<T> initFactory,
            Supplier<T> overflowFactory,
            Consumer<? super T> clearingProcedure) {
        this(
                name,
                Require.geq(size, "size", MIN_SIZE, "MIN_SIZE"),
                makeFactoryAdapter(Require.neqNull(initFactory, "initFactory")),
                makeFactoryAdapter(Require.neqNull(overflowFactory, "overflowFactory")),
                clearingProcedure);
    }

    public ThreadSafeLenientFixedSizePool(String name,
            int size,
            Function<ThreadSafeLenientFixedSizePool<T>, T> initFactory,
            Function<ThreadSafeLenientFixedSizePool<T>, T> overflowFactory,
            Consumer<? super T> clearingProcedure) {
        Require.geq(size, "size", MIN_SIZE, "MIN_SIZE");
        Require.neqNull(initFactory, "initFactory");
        Require.neqNull(overflowFactory, "overflowFactory");
        this.factory = overflowFactory;
        this.clearingProcedure = clearingProcedure;
        this.pool = new LockFreeArrayQueue<T>(MathUtil.ceilLog2(size + 2));
        for (int i = 0; i < size; ++i) {
            pool.enqueue(initFactory.apply(this));
        }
        extraFactoryCalls = name == null ? null : Stats.makeItem(name, "extraFactoryCalls", Counter.FACTORY).getValue();
    }

    public T take() {
        T item = pool.dequeue();
        if (item == null) {
            if (extraFactoryCalls != null) {
                extraFactoryCalls.sample(1);
            }
            return factory.apply(this);
        }
        return item;
    }

    public void give(T item) {
        giveInternal(item);
    }

    protected boolean giveInternal(T item) {
        if (null == item) {
            return false;
        }
        if (null != clearingProcedure) {
            clearingProcedure.accept(item);
        }
        return pool.enqueue(item); // discard if enqueue fails
    }

    @Deprecated
    public T takeMaybeNull() {
        return tryTake();
    }

    @Override
    public T tryTake() {
        return pool.dequeue();
    }
}
