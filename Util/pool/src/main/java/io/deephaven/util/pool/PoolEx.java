//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.pool;

import io.deephaven.base.pool.Pool;

public interface PoolEx<T> extends Pool<T> {
    /**
     * Take an item if immediately available, else return null.
     *
     * @return a pool item or null
     */
    T tryTake();
}
