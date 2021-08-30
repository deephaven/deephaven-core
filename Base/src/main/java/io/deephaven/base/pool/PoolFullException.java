/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.pool;

// --------------------------------------------------------------------
/**
 * Indicates that a {@link Pool} is full and can't be {@link Pool#give give}n more items.
 */
public class PoolFullException extends RuntimeException {

    public PoolFullException() {}

    public PoolFullException(String message) {
        super(message);
    }
}
