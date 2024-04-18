//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.vectors;

/**
 * Repository for constants used by the column-wrapper vector implementations.
 */
public class VectorColumnWrapperConstants {

    /**
     * Size threshold at which column-wrapper vector implementations will use a
     * {@link io.deephaven.engine.table.iterators.ChunkedColumnIterator ChunkedColumnIterator} instead of a
     * {@link io.deephaven.engine.table.iterators.SerialColumnIterator SerialColumnIterator}.
     */
    static final int CHUNKED_COLUMN_ITERATOR_SIZE_THRESHOLD = 1 << 7;
}
