/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.pool;

import io.deephaven.base.Function;
import io.deephaven.base.Procedure;

// --------------------------------------------------------------------
/**
 * Provides a pool of reusable items. Using a pool avoids garbage creation.
 */
public interface Pool<T> {

    /**
     * Takes an item from the pool. Depending on pool policy, if there are no items available, this
     * may block, create a new item, or throw a {@link PoolEmptyException}.
     */
    T take();

    /**
     * Gives an unused item back to the pool. Passing <code>null</code> is safe and has no effect.
     * If the pool has a clearing procedure, the item will be cleared. Depending on pool policy, if
     * the pool is full, this may block, discard the item, or throw a {@link PoolFullException}.
     */
    void give(T item);

    // ----------------------------------------------------------------
    interface Factory {

        /**
         * Creates a new pool.
         * 
         * @param nSize A hint of the maximum number of items expected to be taken from the pool at
         *        once. The behavior when more items are taken depends on the pool. The pool may
         *        preallocate this many items. When the maximum can't be given, use 0.
         * @param itemFactory Creates new items. May be <code>null</code> if items will be
         *        {@link Pool#give give}n rather than created.
         * @param clearingProcedure Called on each item given to the pool to clear the fields of the
         *        item. May be <code>null</code>.
         */
        <T> Pool<T> create(int nSize, Function.Nullary<T> itemFactory,
            Procedure.Unary<T> clearingProcedure);
    }

    /**
     * Must implement at least: One thread may call give(), one thread may call take()
     */
    public static interface SinglePool<T> extends Pool<T> {
    }

    /**
     * Must implement at least: Multiple threads may call give(), one thread may call take()
     */
    public static interface MultiGiver<T> extends SinglePool<T> {
    }

    /**
     * Must implement at least: Multiple threads may call take(), one thread may call give()
     */
    public static interface MultiTaker<T> extends SinglePool<T> {
    }

    /**
     * Must implement at least: Multiple threads may call give(), multiple threads may call take()
     */
    public static interface MultiPool<T> extends MultiGiver<T>, MultiTaker<T> {
    }
}
