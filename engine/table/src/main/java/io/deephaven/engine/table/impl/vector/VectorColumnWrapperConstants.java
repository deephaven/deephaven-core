package io.deephaven.engine.table.impl.vector;

import io.deephaven.configuration.Configuration;

/**
 * Repository for constants used by the column-wrapper vector implementations.
 */
public class VectorColumnWrapperConstants {

    /**
     * Size threshold at which column-wrapper vector implementations will use a
     * {@link io.deephaven.engine.table.iterators.ChunkedColumnIterator ChunkedColumnIterator} instead of a
     * {@link io.deephaven.engine.table.iterators.SerialColumnIterator SerialColumnIterator}.
     */
    static final int CHUNKED_COLUMN_ITERATOR_SIZE_THRESHOLD = Configuration.getInstance().getIntegerWithDefault(
            "VectorColumnWrapper.chunkedColumnIteratorSizeThreshold", 1 << 7);
}
