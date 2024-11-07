//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.iterator;

import io.deephaven.util.SafeCloseable;

import java.util.PrimitiveIterator;

/**
 * Interface for {@link SafeCloseable closeable} {@link PrimitiveIterator primitive iterators}.
 */
public interface CloseablePrimitiveIterator<TYPE, TYPE_CONSUMER>
        extends PrimitiveIterator<TYPE, TYPE_CONSUMER>, CloseableIterator<TYPE> {
}
