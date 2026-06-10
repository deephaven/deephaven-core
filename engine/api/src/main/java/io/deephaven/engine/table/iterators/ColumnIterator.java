//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.primitive.value.iterator.ValueIterator;
import io.deephaven.util.SafeCloseable;

import java.util.function.Consumer;

/**
 * Iteration support for values supplied by a column.
 *
 * @apiNote ColumnIterators must be explicitly {@link #close() closed} or used until exhausted in order to avoid
 *          resource leaks.
 */
public interface ColumnIterator<DATA_TYPE> extends ValueIterator<DATA_TYPE>, SafeCloseable {

    @Override
    default void close() {}

    /**
     * Consumes all of the remaining data from {@code this} iterator. This is useful to force-read column data in an
     * iterative, efficient manner. This is semantically equivalent to:
     *
     * <pre>{@code
     * while (it.hasNext()) {
     *     it.next();
     * }
     * }</pre>
     * 
     * , but callers should prefer to call this method as the primitive implementations will not box, and the iteration
     * may be done in a more efficient, chunked manner.
     */
    void consumeAll();
}
