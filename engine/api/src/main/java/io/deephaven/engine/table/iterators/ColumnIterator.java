//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.util.SafeCloseable;

/**
 * Iteration support for values supplied by a column.
 *
 * @apiNote ColumnIterators must be explicitly {@link #close() closed} or used until exhausted in order to avoid
 *          resource leaks.
 */
public interface ColumnIterator<DATA_TYPE> extends CloseableIterator<DATA_TYPE>, SafeCloseable {

    @Override
    default void close() {}

    /**
     * @return The number of elements remaining in this ColumnIterator
     */
    long remaining();
}
