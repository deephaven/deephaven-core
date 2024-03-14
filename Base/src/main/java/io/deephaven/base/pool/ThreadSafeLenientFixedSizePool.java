//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.pool;

import io.deephaven.base.LockFreeArrayQueue;
import io.deephaven.base.MathUtil;
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
public class ThreadSafeLenientFixedSizePool<T> implements Pool.MultiPool<T> {

    public static Factory FACTORY = new Factory() {
        @Override
        public <T> Pool<T> create(int nSize, Supplier<T> itemFactory, Consumer<T> clearingProcedure) {
            return new ThreadSafeLenientFixedSizePool<T>(Require.geq(nSize, "nSize", MIN_SIZE, "MIN_SIZE"),
                    Require.neqNull(itemFactory, "itemFactory"), clearingProcedure);
        }
    };

    private static <T> Function<ThreadSafeLenientFixedSizePool<T>, T> makeNullaryFactoryAdapter(
            final Supplier<T> factory) {
        return arg -> factory.get();
    }

    public static final int MIN_SIZE = 7;

    private final LockFreeArrayQueue<T> pool; // TODO: should be a stack
    private final Function<ThreadSafeLenientFixedSizePool<T>, T> factory;
    private final Consumer<? super T> clearingProcedure;
    private final Counter extraFactoryCalls;

    public ThreadSafeLenientFixedSizePool(int size, Supplier<T> factory,
            Consumer<? super T> clearingProcedure) {
        this(
                Require.geq(size, "size", MIN_SIZE, "MIN_SIZE"),
                makeNullaryFactoryAdapter(Require.neqNull(factory, "factory")),
                clearingProcedure);
    }

    public ThreadSafeLenientFixedSizePool(String name, int size, Supplier<T> factory,
            Consumer<? super T> clearingProcedure) {
        this(
                name,
                Require.geq(size, "size", MIN_SIZE, "MIN_SIZE"),
                makeNullaryFactoryAdapter(Require.neqNull(factory, "factory")),
                clearingProcedure);
    }

    public ThreadSafeLenientFixedSizePool(
            int size,
            Function<ThreadSafeLenientFixedSizePool<T>, T> factory,
            Consumer<? super T> clearingProcedure) {
        this(null, size, factory, clearingProcedure);
    }

    public ThreadSafeLenientFixedSizePool(
            String name,
            int size,
            Function<ThreadSafeLenientFixedSizePool<T>, T> factory,
            Consumer<? super T> clearingProcedure) {
        Require.geq(size, "size", MIN_SIZE, "MIN_SIZE");
        Require.neqNull(factory, "factory");
        this.factory = factory;
        this.clearingProcedure = clearingProcedure;
        this.pool = new LockFreeArrayQueue<T>(MathUtil.ceilLog2(size + 2));
        for (int i = 0; i < size; ++i) {
            pool.enqueue(factory.apply(this));
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

    public T takeMaybeNull() {
        return pool.dequeue();
    }
}
