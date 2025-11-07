//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.primitive.value.iterator.ValueIterator;
import io.deephaven.util.SafeCloseable;

/**
 * Iteration support for values supplied by a column.
 *
 * @apiNote ColumnIterators must be explicitly {@link #close() closed} or used until exhausted in order to avoid
 *          resource leaks.
 */
public interface ColumnIterator<DATA_TYPE> extends ValueIterator<DATA_TYPE>, SafeCloseable {

    @Override
    default void close() {}

}
