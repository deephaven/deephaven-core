/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.pool;

// --------------------------------------------------------------------
/**
 * Indicates that a {@link Pool} is empty and no more items can be {@link Pool#take}n.
 */
public class PoolEmptyException extends RuntimeException {

    public PoolEmptyException() {}

    public PoolEmptyException(String message) {
        super(message);
    }
}
